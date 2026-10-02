# task26100361

可断点续跑的 SQLite 表 → CSV 迁移器（仅标准库）。

## API

```python
from migrator import create_job, run_job, get_job

spec = {
    "source_path": "src.db",          # 源库文件路径
    "table": "t",                     # 源表名
    "columns": [                      # 输出列：源列名 / 输出列名 / 目标类型
        {"source": "id", "target": "ID", "type": "integer"},
        {"source": "name", "target": "Name", "type": "text"},
    ],
    "output_path": "out.csv",         # 目标 CSV 路径（原地增长）
    "batch_size": 1000,               # <= 1000
}
job_id = create_job("meta.db", spec)  # 登记作业，初始状态 queued
run_job("meta.db", job_id)            # 从上次进度续跑，直到 completed / partially_complete
info = get_job("meta.db", job_id)     # 状态、已写行数、暂停时的失败信息
```

## CLI（可在子进程中执行 RunJob）

```sh
python -m migrator create META.db spec.json
python -m migrator run    META.db JOB_ID
python -m migrator get    META.db JOB_ID
```

## 断点续跑原理

- 进度（已提交的文件偏移、最后写出的主键、已写行数）存在独立的元数据 SQLite 文件中。
- 每批写完后 flush + fsync，再提交元数据；恢复时把目标文件截断到已提交偏移，
  被 SIGKILL 打断的半批直接重写，保证最终产物不重不漏。
- 源表按主键（或 rowid）keyset 分页读取，不用 OFFSET，恢复代价与已迁移数据量无关。
- 某行类型转换失败时整批不写出，作业停在 partially_complete，并记录该批首末行主键、
  第一个出错行主键与出错原因；修复源表后再次 run_job 即从该批继续。

## 测试

```sh
python -m unittest discover -s tests -v
```
