from python.core.configs import get_job_config
from python.core.utils import get_fields_for

job_cfg = get_job_config('warehouse_order_logs')
# print(job_cfg.map)

field = get_fields_for('source', job_cfg.map)
print(field)