from sqlalchemy import desc
from sqlalchemy.sql import func

from db.entities.stage_statistics import StageStatistics
from db.entities.task import Task
from sparkscope_web.analyzers.analyzer import Analyzer
from db.entities.stage import Stage
from sparkscope_web.metrics.metric import StageFailureMetric, EmptyMetric, StageSkewMetric, StageDiskSpillMetric
from sparkscope_web.metrics.metrics_constants import STAGE_SKEW_MIN_RUNTIME_MILLIS, STAGE_SKEW_THRESHOLDS, \
    STAGE_DISK_SPILL_THRESHOLDS
from sparkscope_web.metrics.severity import Severity


class StageAnalyzer(Analyzer):
    def __init__(self, app):
        super().__init__()
        self.stages = self.db.query(Stage).filter(Stage.app_id == app.app_id)
        self.stages_with_statistics = self.stages.join(StageStatistics)

    def analyze_failed_stages(self):
        """
        Analyze the Stages of the defined apps and return the metric details
        :return: Metric details
        """

        all_stages_count = self.stages.count()
        if all_stages_count == 0:
            return EmptyMetric(severity=Severity.NONE)
        failed_stages = self.stages.filter(Stage.status.in_(["FAILED", "KILLED"]))
        failed_stages_count = failed_stages.count()

        if failed_stages_count > 0:
            severity = Severity.HIGH
            overall_info = f"{failed_stages_count}/{all_stages_count} stages failed"
            details = []
            for fs in failed_stages.all():
                details.append(f"Stage {fs.stage_id} {fs.status}: {fs.failure_reason}")
            return StageFailureMetric(severity, overall_info, details)
        else:
            return EmptyMetric(severity=Severity.NONE)

    def analyze_stage_skews(self):
        # filter only relevant stages (don't bother with five-second stages, focus on the large ones)
        relevant_stages = self.stages.filter(Stage.executor_run_time > STAGE_SKEW_MIN_RUNTIME_MILLIS)

        severity = Severity.NONE  # just an initialization; will be updated
        details = {}  # the details can be sorted by (max - median) difference, which might represent potential time loss
        for stage in relevant_stages.all():
            idx = [2, 4]  # indexes for median and maximum
            runtime_med, runtime_max = [stage.stage_statistics.executor_run_time[i]/1000 for i in idx]  # milliseconds!
            bytes_read_med, bytes_read_max = [stage.stage_statistics.bytes_read[i] for i in idx]
            bytes_written_med, bytes_written_max = [stage.stage_statistics.bytes_written[i] for i in idx]
            shuffle_read_bytes_med, shuffle_read_bytes_max = [stage.stage_statistics.shuffle_read_bytes[i] for i in idx]
            shuffle_write_bytes_med, shuffle_write_bytes_max = [stage.stage_statistics.shuffle_write_bytes[i] for i in idx]

            # severity should be calculated from runtime skew
            if runtime_med == 0:  # check division by 0
                stage_severity = Severity.HIGH
            else:
                stage_severity = STAGE_SKEW_THRESHOLDS.severity_of(runtime_max/runtime_med)

            if stage_severity > severity:
                severity = stage_severity

            if stage_severity > Severity.NONE:
                details[f"Stage {stage.stage_id}: Executor runtime {runtime_max} s (max), {runtime_med} s (median)"
                        f"Read {bytes_read_max} B (max), {bytes_read_med} B (median)"
                        f"Wrote {bytes_written_max} B (max), {bytes_written_med} B (median))"
                        f"Shuffle read {shuffle_read_bytes_max} B (max), {shuffle_read_bytes_med} B (median)"
                        f"Shuffle write {shuffle_write_bytes_max} B (max), {shuffle_write_bytes_max} B (median)"] = \
                        runtime_max - runtime_med
        #         TODO think about some better data structure for storing the details

        # TODO sort the details by (runtime_max - runtime_med) and display some limited number of such stages (5?)
        # TODO also, some generic method for getting the details could help

        if len(details) == 0:  # no stages with significant severity
            return EmptyMetric(severity=Severity.NONE)
        else:
            overall_info = f"{len(details)} stages with significant skew found"
            return StageSkewMetric(severity=severity, overall_info=overall_info, details=details)

    def analyze_disk_spills(self):
        # filter only relevant stages (with non-zero spill)
        relevant_stages = self.stages.filter(Stage.memory_bytes_spilled > 0)

        severity = Severity.NONE
        details = {}  # the details can be sortable by amount of spilled data per stage

        total_bytes_spilled = 0
        stages_count = 0

        for stage in relevant_stages.all():
            memory_bytes_spilled = stage.memory_bytes_spilled
            disk_bytes_spilled = stage.disk_bytes_spilled
            input_bytes = stage.input_bytes
            output_bytes = stage.output_bytes
            shuffle_read_bytes = stage.shuffle_read_bytes
            shuffle_write_bytes = stage.shuffle_write_bytes
            stage_key = stage.stage_key
            id = stage.stage_id

            # also find which tasks is the most responsible for the spill
            worst_task = self.db.query(Task).filter(Task.stage_key == stage_key)\
                .order_by(desc(Task.memory_bytes_spilled)).first()
            memory_bytes_spilled_by_worst_task = worst_task.memory_bytes_spilled
            disk_bytes_spilled_by_worst_task = worst_task.disk_bytes_spilled

            max_memory_usage = max(input_bytes, output_bytes, shuffle_read_bytes, shuffle_write_bytes)
            stage_severity = STAGE_DISK_SPILL_THRESHOLDS.severity_of(memory_bytes_spilled/max_memory_usage)

            total_bytes_spilled += memory_bytes_spilled
            stages_count += 1

            if stage_severity > severity:
                severity = stage_severity

            if stage_severity > Severity.NONE:
                details[id] = (f"Stage {id} spilled {memory_bytes_spilled} bytes ({disk_bytes_spilled} on disk)."
                               f"Input: {input_bytes} B, output: {output_bytes} B. "
                               f"Shuffle read: {shuffle_read_bytes} B, shuffle write: {shuffle_write_bytes} B."
                               f"Biggest contributor: task {worst_task.task_id}, {memory_bytes_spilled_by_worst_task} B spilled ({disk_bytes_spilled_by_worst_task} on disk).",
                               memory_bytes_spilled)

        if len(details) == 0:
            return EmptyMetric(severity=Severity.NONE)
        else:
            overall_info = f"{total_bytes_spilled} bytes spilled in {stages_count} stages."
            return StageDiskSpillMetric(severity=severity, overall_info=overall_info, details=details)
