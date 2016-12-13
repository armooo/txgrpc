from __future__ import absolute_import
from __future__ import print_function
from __future__ import division

from txgrpc import stats


def test_null_success():
    s = stats.NullStats()
    s.log_success(10)
    s.log_success(45)


def test_null_error():
    s = stats.NullStats()
    s.log_error(10)
    s.log_error(45)


def test_local_success():
    s = stats.LocalStats()
    for i in range(100):
        s.log_success(i)

    assert s.get_stats()['calls'] == 100
    assert s.get_stats()['min'] == 0
    assert s.get_stats()['max'] == 99
    assert s.get_stats()['avg'] == 49.5
    assert s.get_stats()['p75'] == 75
    assert s.get_stats()['p90'] == 90
    assert s.get_stats()['p95'] == 95


def test_local_reset():
    s = stats.LocalStats()
    for i in range(100):
        s.log_success(i)

    assert s.get_stats()['calls'] == 100

    s.reset()

    assert s.get_stats()['calls'] == 0
    assert s.get_stats()['min'] == 0
    assert s.get_stats()['min'] == 0
    assert s.get_stats()['avg'] == 0
    assert s.get_stats()['p75'] == 0
    assert s.get_stats()['p90'] == 0
    assert s.get_stats()['p95'] == 0


def test_local_error():
    s = stats.LocalStats()
    s.log_error(10)
    s.log_error(45)
