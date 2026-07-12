
❯ làm sao để biết hệ thống đg chạy nhiều worker cùng lúc

Có vài cách nhanh để kiểm tra, từ đơn giản đến chi tiết:

1. Celery inspect — xem worker đang chạy gì ngay lúc này
# Ping tất cả worker đang online
uv run --project api celery -A app.celery inspect ping

# Xem task đang active trên từng worker
uv run --project api celery -A app.celery inspect active

# Xem queue mỗi worker đang lắng nghe
uv run --project api celery -A app.celery inspect active_queues

# Xem stats (concurrency, pool, processed count)
uv run --project api celery -A app.celery inspect stats

2. Redis — xem queue depth real-time
# Xem số task đang chờ trong queue workflow
watch -n 1 'docker compose -p dify exec redis redis-cli -a difyai123456 llen workflow'

# Xem tất cả queues cùng lúc
watch -n 1 'docker compose -p dify exec redis redis-cli -a difyai123456 keys "*" | sort'

3. Flower — UI trực quan nhất
# Nếu chưa chạy, start Flower
uv run --project api celery -A app.celery flower --port=5555 &

# Mở browser
open http://localhost:5555
Flower hiện: mỗi worker là 1 row, cột Active tăng khi có task đang chạy, cột Processed đếm tổng đã xử lý.

4. Kết hợp với bench để thấy rõ
# Terminal 1: fire 20 runs concurrent
python3 bench-run.py --workflow code -n 20 -c 10

# Terminal 2: watch queue + worker cùng lúc
watch -n 0.5 'docker compose -p dify exec redis redis-cli -a difyai123456 llen workflow && \
  uv run --project api celery -A app.celery inspect active 2>/dev/null | grep -c "worker_pid" || echo "checking..."'

Hiện tại bạn chỉ có 1 worker process (PID 71107 từ lúc start). Khi chạy inspect active sẽ thấy 1 node duy nhất. Nếu muốn thấy nhiều worker thật sự, start thêm:

# Worker thứ 2 (terminal mới)
dev/start-worker -q workflow,pipeline -c 4

# Rồi inspect ping — sẽ thấy 2 node response
uv run --project api celery -A app.celery inspect ping