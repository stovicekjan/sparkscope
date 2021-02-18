from sqlalchemy import desc
from sqlalchemy.sql import func

from db.entities.stage_statistics import StageStatisticsEntity
from db.entities.task import TaskEntity
from sparkscope_web.analyzers.analyzer import Analyzer
from db.entities.stage import StageEntity
from sparkscope_web.metrics.helpers import fmt_bytes, fmt_time
from sparkscope_web.metrics.metric import StageFailureMetric, EmptyMetric, StageSkewMetric, StageDiskSpillMetric
from sparkscope_web.metrics.metric_details import MetricDetailsList, MetricDetails
from sparkscope_web.metrics.metrics_constants import STAGE_SKEW_MIN_RUNTIME_MILLIS, STAGE_SKEW_THRESHOLDS, \
    STAGE_DISK_SPILL_THRESHOLDS
from sparkscope_web.metrics.severity import Severity


class StageAnalyzer(Analyzer):
    def __init__(self, app):
        super().__init__()
        self.stages = self.db.query(StageEntity.status,
                                    StageEntity.stage_id,
                                    StageEntity.stage_key,
                                    StageEntity.failure_reason,
                                    StageEntity.executor_run_time,
                                    StageEntity.memory_bytes_spilled,
                                    StageEntity.disk_bytes_spilled,
                                    StageEntity.input_bytes,
                                    StageEntity.output_bytes,
                                    StageEntity.shuffle_read_bytes,
                                    StageEntity.shuffle_write_bytes,
                                    StageStatisticsEntity.executor_run_time.label("ss_executor_run_time"),
                                    StageStatisticsEntity.bytes_read.label("ss_bytes_read"),
                                    StageStatisticsEntity.bytes_written.label("ss_bytes_written"),
                                    StageStatisticsEntity.shuffle_read_bytes.label("ss_shuffle_read_bytes"),
                                    StageStatisticsEntity.shuffle_write_bytes.label("ss_shuffle_write_bytes")) \
                             .filter(StageEntity.app_id == app.app_id)\
                             .join(StageStatisticsEntity,
                                   StageEntity.stage_key == StageStatisticsEntity.stage_key,
                                   isouter=True)\
                             .all()

    def analyze_failed_stages(self):
        """
        Analyze the Stages of the defined apps and return the metric details
        :return: Metric details
        """
        all_stages_count = len(self.stages)
        if all_stages_count == 0:
            return EmptyMetric(severity=Severity.NONE)
        failed_stages = [s for s in self.stages if s.status in ["FAILED", "KILLED"]]
        failed_stages_count = len(failed_stages)

        if failed_stages_count > 0:
            severity = Severity.HIGH
            overall_info = f"{failed_stages_count}/{all_stages_count} stages failed"
            details = MetricDetailsList(ascending=True)
            for fs in failed_stages:
                details.add(MetricDetails(entity_id=fs.stage_id,
                                          detail_string=f"Stage {fs.stage_id} {fs.status}",
                                          subdetails=[fs.failure_reason]))

            return StageFailureMetric(severity, overall_info, details)
        else:
            return EmptyMetric(severity=Severity.NONE)

    def analyze_stage_skews(self):
        # filter only relevant stages (don't bother with five-second stages, focus on the large ones)
        relevant_stages = [s for s in self.stages if s.executor_run_time > STAGE_SKEW_MIN_RUNTIME_MILLIS]

        severity = Severity.NONE  # just an initialization; will be updated
        details = MetricDetailsList(ascending=True)
        for stage in relevant_stages:
            if stage.ss_executor_run_time is None:  # sometimes the stage statistics data are not available
                return EmptyMetric(severity=Severity.NONE)

            idx = [2, 4]  # indexes for median and maximum
            runtime_med, runtime_max = [stage.ss_executor_run_time[i]/1000 for i in idx]  # seconds
            bytes_read_med, bytes_read_max = [stage.ss_bytes_read[i] for i in idx]
            bytes_written_med, bytes_written_max = [stage.ss_bytes_written[i] for i in idx]
            shuffle_read_bytes_med, shuffle_read_bytes_max = [stage.ss_shuffle_read_bytes[i] for i in idx]
            shuffle_write_bytes_med, shuffle_write_bytes_max = [stage.ss_shuffle_write_bytes[i] for i in idx]
            id = stage.stage_id
            total_runtime = stage.executor_run_time/1000  # seconds

            # severity should be calculated from runtime skew
            if runtime_med == 0:  # check division by 0
                stage_severity = Severity.HIGH
            else:
                stage_severity = STAGE_SKEW_THRESHOLDS.severity_of(runtime_max/runtime_med)

            if stage_severity > severity:
                severity = stage_severity

            if stage_severity > Severity.NONE:
                details.add(MetricDetails(entity_id=id,
                                          detail_string=f"Stage {stage.stage_id}: Total runtime {fmt_time(total_runtime)}, Executor runtime {fmt_time(runtime_max)} (max), {fmt_time(runtime_med)} (median)",
                                          sort_attr=runtime_max - runtime_med, # the details can be sorted by (max - median) difference, which might represent potential time loss
                                          subdetails=[f"Read {fmt_bytes(bytes_read_max)} (max), {fmt_bytes(bytes_read_med)} (median)",
                                                      f"Wrote {fmt_bytes(bytes_written_max)} (max), {fmt_bytes(bytes_written_med)} (median)",
                                                      f"Shuffle read {fmt_bytes(shuffle_read_bytes_max)} (max), {fmt_bytes(shuffle_read_bytes_med)} (median)",
                                                      f"Shuffle write {fmt_bytes(shuffle_write_bytes_max)} (max), {fmt_bytes(shuffle_write_bytes_max)} (median)"]))

        if details.length() == 0:  # no stages with significant severity
            return EmptyMetric(severity=Severity.NONE)
        else:
            overall_info = f"{details.length()} stages with significant skew found"
            return StageSkewMetric(severity=severity, overall_info=overall_info, details=details)

    def analyze_disk_spills(self):
        # filter only relevant stages (with non-zero spill)
        relevant_stages = [s for s in self.stages if s.memory_bytes_spilled > 0]

        severity = Severity.NONE
        details = MetricDetailsList(ascending=True)  # the details can be sortable by amount of spilled data per stage

        total_bytes_spilled = 0
        stages_count = 0

        for stage in relevant_stages:
            memory_bytes_spilled = stage.memory_bytes_spilled
            disk_bytes_spilled = stage.disk_bytes_spilled
            input_bytes = stage.input_bytes
            output_bytes = stage.output_bytes
            shuffle_read_bytes = stage.shuffle_read_bytes
            shuffle_write_bytes = stage.shuffle_write_bytes
            stage_key = stage.stage_key
            id = stage.stage_id

            # also find which tasks is the most responsible for the spill
            worst_task = self.db.query(TaskEntity).filter(TaskEntity.stage_key == stage_key)\
                .order_by(desc(TaskEntity.memory_bytes_spilled)).first()
            memory_bytes_spilled_by_worst_task = worst_task.memory_bytes_spilled
            disk_bytes_spilled_by_worst_task = worst_task.disk_bytes_spilled

            max_memory_usage = max(input_bytes, output_bytes, shuffle_read_bytes, shuffle_write_bytes)
            stage_severity = STAGE_DISK_SPILL_THRESHOLDS.severity_of(memory_bytes_spilled/max_memory_usage)

            total_bytes_spilled += memory_bytes_spilled
            stages_count += 1

            if stage_severity > severity:
                severity = stage_severity

            if stage_severity > Severity.NONE:
                details.add(MetricDetails(entity_id=id,
                                          detail_string=f"Stage {id} spilled {fmt_bytes(memory_bytes_spilled)} ({fmt_bytes(disk_bytes_spilled)} on disk).",
                                          sort_attr=memory_bytes_spilled,
                                          subdetails=[f"Input: {fmt_bytes(input_bytes)}, output: {fmt_bytes(output_bytes)}.",
                                                      f"Shuffle read: {fmt_bytes(shuffle_read_bytes)}, shuffle write: {fmt_bytes(shuffle_write_bytes)}.",
                                                      f"Biggest contributor: task {worst_task.task_id}, {fmt_bytes(memory_bytes_spilled_by_worst_task)} spilled ({fmt_bytes(disk_bytes_spilled_by_worst_task)} on disk)."]
                                          ))

        if details.length() == 0:
            return EmptyMetric(severity=Severity.NONE)
        else:
            overall_info = f"{fmt_bytes(total_bytes_spilled)} spilled in {stages_count} stages."
            return StageDiskSpillMetric(severity=severity, overall_info=overall_info, details=details)
