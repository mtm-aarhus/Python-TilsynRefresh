"""Which Vejman cases are parklets (vejman.is_parklet).

Vejman has no type of its own for parklets: they are udstyr of type "Andet"
with "Parklet" in the description, which is free text. The robot stores the
answer as is_parklet, and TilsynAPI leaves parklets off the weekly inspection
list from October to March."""
import copy
import unittest

from vejman import build_vejman_case, is_parklet

# getcase for a parklet, as Vejman returns it - only the relevant fields.
PARKLET = {
    "func": "getcase",
    "data": {
        "rovm": {
            "equipment": [
                {
                    "type": {"id": 10, "code": "10", "value": "Andet"},
                    "position": 1,
                    "quantity": 1,
                    "unitprice": 0.0,
                    "rowIdentifier": 247213,
                    "description": "Parklet",
                    "id": 101180177,
                }
            ],
            "supervisions": [],
        },
        "id": 101180155,
    },
    "exitcode": 0,
    "errorStr": None,
}


def details(*rows):
    return {"rovm": {"equipment": list(rows)}}


def row(kind, description):
    return {"type": {"id": 10, "code": "10", "value": kind}, "description": description}


class IsParklet(unittest.TestCase):
    def test_andet_described_as_parklet(self):
        self.assertTrue(is_parklet(PARKLET["data"]))

    def test_the_description_is_trimmed_and_case_does_not_matter(self):
        for text in (" Parklet ", "PARKLET", "parklet\n", "\tParKlet"):
            with self.subTest(text=text):
                self.assertTrue(is_parklet(details(row("Andet", text))))

    def test_the_type_is_trimmed_and_case_does_not_matter(self):
        self.assertTrue(is_parklet(details(row(" andet ", "Parklet"))))

    def test_one_parklet_among_other_equipment(self):
        self.assertTrue(is_parklet(details(row("Container", "Affald"), row("Andet", "Parklet"))))

    def test_only_the_word_parklet_on_its_own(self):
        for text in ("Parkletter", "Parklet og stole", "Ikke parklet", "Andet", "", None):
            with self.subTest(text=text):
                self.assertFalse(is_parklet(details(row("Andet", text))))

    def test_parklet_under_another_type_is_not_one(self):
        self.assertFalse(is_parklet(details(row("Stillads", "Parklet"))))

    def test_a_case_without_equipment(self):
        for case in ({}, None, {"rovm": None}, {"rovm": {}}, {"rovm": {"equipment": None}},
                     {"rovm": {"equipment": []}}):
            with self.subTest(case=case):
                self.assertFalse(is_parklet(case))

    def test_a_type_given_as_plain_text_counts_too(self):
        self.assertTrue(is_parklet({"rovm": {"equipment": [{"type": "Andet", "description": "Parklet"}]}}))

    def test_odd_shapes_do_not_stop_the_run(self):
        for case in ({"rovm": "Parklet"}, {"rovm": {"equipment": "Parklet"}},
                     {"rovm": {"equipment": [None, "Parklet", {"type": None, "description": None}]}}):
            with self.subTest(case=case):
                self.assertFalse(is_parklet(case))


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


class FakeSession:
    """Answers getcase with one fixed payload."""

    def __init__(self, payload):
        self.payload = payload

    def get(self, url, timeout=None):
        return FakeResponse(self.payload)


# The case as the getcases list has it.
LISTED = {
    "case_id": 101180155,
    "case_number": "2026-012345",
    "state": "3",
    "street_name": "Mejlgade",
    "start_date": "01-04-2026",
    "end_date": "30-09-2026",
    "initials": "MAMASA",
}


class BuildCase(unittest.TestCase):
    def test_a_parklet_case_says_so(self):
        case = build_vejman_case(FakeSession(PARKLET), "token", dict(LISTED))
        self.assertIs(case["is_parklet"], True)

    def test_any_other_case_says_not(self):
        payload = copy.deepcopy(PARKLET)
        payload["data"]["rovm"]["equipment"][0]["description"] = "Skurvogn"
        case = build_vejman_case(FakeSession(payload), "token", dict(LISTED))
        self.assertIs(case["is_parklet"], False)

    def test_a_gravetilladelse_has_no_rovm_and_is_no_parklet(self):
        case = build_vejman_case(FakeSession({"data": {"id": 1}}), "token", dict(LISTED))
        self.assertIs(case["is_parklet"], False)


if __name__ == "__main__":
    unittest.main()
