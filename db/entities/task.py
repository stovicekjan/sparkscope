# # coding=utf-8
#
# from sqlalchemy import Column, String, Integer, Date, DateTime, BigInteger, Boolean, JSON, ARRAY, ForeignKey, Float
# from sqlalchemy.orm import relationship
#
# from db.base import Base
#
#
# class Task(Base):
#     __tablename__ = 'task'
#
#     task_key = Column(String)
#     stage_key = Column(String)
#     app_id = Column(String)
#     stage_id = Column(Integer)
#     task_id = Column(Integer)
#     index = Column(Integer)
#     attempt = Column(Integer)
#     launch_time = Column(DateTime)
#     duration = Column(BigInteger)
#     executor_key = Column(String)
#     host = Column(String)
#     status = Column(String)
#     task_locality = Column(String)
#     speculative = Column(Boolean)
#     accumulator_updates = Column(ARRAY(String))
#     executor_deserialize_time = Column(BigInteger)
#     executor_deserialize_cpu_time = Column(BigInteger)
#     executor_run_time = Column(BigInteger)
#     executor_cpu_time = Column(BigInteger)
#     result_size = Column(BigInteger)
#     jvm_gc_time = Column(BigInteger)
#     result_serialization_time = Column(BigInteger)
#     memory_bytes_spilled = Column(BigInteger)
#     disk_bytes_spilled = Column(BigInteger)
#     peak_execution_memory = Column(BigInteger)
#     bytes_read = Column(BigInteger)
#     records_read = Column(BigInteger)
#     bytes_written = Column(BigInteger)
#     records_written = Column(BigInteger)
#     shuffle_remote_blocks_fetched = Column(BigInteger)
#     shuffle_local_blocks_fetched = Column(BigInteger)
#     shuffle_fetch_wait_time = Column(BigInteger)
#     shuffle_remote_bytes_read = Column(BigInteger)
#     shuffle_remote_bytes_read_to_disk = Column(BigInteger)
#     shuffle_local_bytes_read = Column(BigInteger)
#     shuffle_records_read = Column(BigInteger)
#     shuffle_bytes_written = Column(BigInteger)
#     shuffle_write_time = Column(BigInteger)
#     shuffle_records_written = Column(BigInteger)
#
#
#
#
#     def __init__(self, attributes):
#         pass

