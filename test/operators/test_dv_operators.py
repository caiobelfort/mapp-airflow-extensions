from mapp.operators.dv_operators import _format_business_keys


class TestFormatBusinessKeyFunction(object):

    def test_return_list_of_lists(self):
        """Tests if the function returns a list of lists"""

        row = {"bk1": 'val', "bk2": "val", "bk3": "val", "bk4": "val", "bk5": "val"}

        bk = [["bk1"], "bk2", ["bk3", "bk4"], "bk5"]

        formatted_bks = _format_business_keys(row, bk)

        # Tests if returned result is a list
        assert isinstance(formatted_bks, list)

        # Testes if each object in list is also a list
        assert all(isinstance(x, list) for x in formatted_bks)

    def test_return_strs(self):
        row = {"bk1": 2, "bk2": 3, "bk3": "val", "bk4": "val", "bk5": "val"}

        bk = [["bk1"], "bk2", ["bk3", "bk4"], "bk5"]

        formatted_bks = _format_business_keys(row, bk)

        assert all(isinstance(v, str) for sl in formatted_bks for v in sl)

    def test_correct_result(self):
        row = {"bk1": 2, "bk2": 3, "bk3": "val", "bk4": "val", "bk5": "val"}

        bk = [["bk1"], "bk2", ["bk3", "bk4"], "bk5"]

        formatted_bks = _format_business_keys(row, bk)

        expect = [["2"], ["3"], ["val", "val"], ["val"]]

        assert formatted_bks == expect
