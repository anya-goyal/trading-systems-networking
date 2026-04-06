Related changes (supporting the tests, not “integration tests” by themselves)
internal_order_gateway: Windows connect_ex in-progress handling, _exchange_config() so patched ports work even if the module was imported earlier, and forwarding 150=4 → RESP_CANCELLED.
exchange1/engine.py: cancel path bugfix (_send_er no invalid cum_qty= kwarg).
tests/test_exec_handler.py: unit test that exec_type 4 sets order state to cancelled.