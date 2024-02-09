
from ..gain_tracker import position

class TestPosition:
    def test_init(self):
        my_p = position.Position("T", None)
        print(my_p.status)
        assert my_p.status.value == 0
        assert my_p.external_id == "T"