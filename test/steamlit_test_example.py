# tests/test_member_overview_page.py
from streamlit.testing.v1 import AppTest

def test_member_overview_page_renders():
    at = AppTest.from_file("utility/app.py")
    at.run()

    assert len(at.title) >= 1
    assert "Dáil" in at.title[0].value
    assert len(at.dataframe) >= 1
    assert len(at.download_button) >= 1