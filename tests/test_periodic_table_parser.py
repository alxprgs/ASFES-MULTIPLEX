from __future__ import annotations

from datetime import UTC, datetime

from server.core.internal_data._parse_periodic_table import (
    build_periodic_table_dataset,
    merge_element_records,
    parse_element_page,
    parse_periodic_table_page,
)


RU_TABLE_HTML = """
<html>
  <body>
    <h1 id="firstHeading">Периодическая система химических элементов</h1>
    <div class="mw-parser-output">
      <p>Периодическая система объединяет химические элементы по их атомному номеру.</p>
      <h2><span class="mw-headline">Группы</span></h2>
      <p>Группы объединяют элементы со схожими химическими свойствами.</p>
      <table class="wikitable">
        <tr>
          <td>1 <a href="/wiki/%D0%92%D0%BE%D0%B4%D0%BE%D1%80%D0%BE%D0%B4">Водород</a> H</td>
          <td>2 <a href="/wiki/%D0%93%D0%B5%D0%BB%D0%B8%D0%B9">Гелий</a> He</td>
        </tr>
      </table>
    </div>
  </body>
</html>
"""

EN_TABLE_HTML = """
<html>
  <body>
    <h1 id="firstHeading">Periodic table</h1>
    <div class="mw-parser-output">
      <p>The periodic table arranges the chemical elements by atomic number.</p>
      <h2><span class="mw-headline">Blocks</span></h2>
      <p>Blocks follow the valence shell being filled.</p>
      <table class="wikitable">
        <tr>
          <td>1 <a href="/wiki/Hydrogen">Hydrogen</a> H</td>
          <td>2 <a href="/wiki/Helium">Helium</a> He</td>
        </tr>
      </table>
    </div>
  </body>
</html>
"""

RU_HYDROGEN_HTML = """
<html>
  <body>
    <h1 id="firstHeading">Водород</h1>
    <table class="infobox">
      <tr><th>Символ</th><td>H</td></tr>
      <tr><th>Атомный номер</th><td>1</td></tr>
      <tr><th>Группа</th><td>1</td></tr>
      <tr><th>Период</th><td>1</td></tr>
      <tr><th>Блок</th><td>s</td></tr>
      <tr><th>Атомная масса</th><td>1,008 а.е.м.</td></tr>
      <tr><th>Электронная конфигурация</th><td>1s1</td></tr>
      <tr><th>Теплопроводность</th><td>0,1805 Вт/(м·К)</td></tr>
      <tr><th>Номер CAS</th><td>1333-74-0</td></tr>
      <tr><th>Наиболее долгоживущие изотопы</th><td>1H, 2H</td></tr>
    </table>
    <div class="mw-parser-output">
      <p>Водород является самым лёгким химическим элементом.</p>
      <h2><span class="mw-headline">Получение</span></h2>
      <p>В лаборатории водород получают действием кислот на металлы. В домашних условиях применяют электролиз.</p>
      <h2><span class="mw-headline">Химические свойства</span></h2>
      <p>Водород проявляет восстановительные свойства и образует соединения со многими элементами.</p>
      <h2><span class="mw-headline">Изотопы</span></h2>
      <table class="wikitable">
        <tr><th>Изотоп</th><th>Период полураспада</th></tr>
        <tr><td>1H</td><td>Стабилен</td></tr>
        <tr><td>3H</td><td>12,32 года</td></tr>
      </table>
    </div>
  </body>
</html>
"""

EN_HYDROGEN_HTML = """
<html>
  <body>
    <h1 id="firstHeading">Hydrogen</h1>
    <table class="infobox">
      <tr><th>Symbol</th><td>H</td></tr>
      <tr><th>Atomic number</th><td>1</td></tr>
      <tr><th>Group</th><td>1</td></tr>
      <tr><th>Period</th><td>1</td></tr>
      <tr><th>Block</th><td>s</td></tr>
      <tr><th>Molar mass</th><td>1.008 g/mol</td></tr>
      <tr><th>Ionization energy</th><td>1312 kJ/mol</td></tr>
    </table>
    <div class="mw-parser-output">
      <p>Hydrogen is the lightest element.</p>
      <h2><span class="mw-headline">Purification</span></h2>
      <p>Hydrogen can be purified by diffusion and getter materials.</p>
    </div>
  </body>
</html>
"""

RU_HELIUM_HTML = """
<html>
  <body>
    <h1 id="firstHeading">Гелий</h1>
    <table class="infobox">
      <tr><th>Символ</th><td>He</td></tr>
      <tr><th>Атомный номер</th><td>2</td></tr>
      <tr><th>Группа</th><td>18</td></tr>
      <tr><th>Период</th><td>1</td></tr>
      <tr><th>Блок</th><td>s</td></tr>
      <tr><th>Атомная масса</th><td>4,0026 а.е.м.</td></tr>
    </table>
    <div class="mw-parser-output">
      <p>Гелий является благородным газом.</p>
    </div>
  </body>
</html>
"""

EN_HELIUM_HTML = """
<html>
  <body>
    <h1 id="firstHeading">Helium</h1>
    <table class="infobox">
      <tr><th>Symbol</th><td>He</td></tr>
      <tr><th>Atomic number</th><td>2</td></tr>
      <tr><th>Group</th><td>18</td></tr>
      <tr><th>Period</th><td>1</td></tr>
      <tr><th>Block</th><td>s</td></tr>
      <tr><th>Molar mass</th><td>4.0026 g/mol</td></tr>
    </table>
    <div class="mw-parser-output">
      <p>Helium is a noble gas.</p>
    </div>
  </body>
</html>
"""

LIVE_STYLE_RU_HTML = """
<html>
  <body>
    <h1 id="firstHeading">Водород</h1>
    <table class="infobox">
      <tr><th>Название, символ, номер</th><td>Водород / Hydrogenium (H), 1</td></tr>
      <tr><th>Группа, период, блок</th><td>1, 1, s-элемент</td></tr>
      <tr><th>Радиус атома</th><td>53 пм</td></tr>
    </table>
    <div class="mw-parser-output">
      <p>Водород является самым лёгким химическим элементом.</p>
    </div>
  </body>
</html>
"""


def test_parse_periodic_table_page_extracts_summary_and_candidates() -> None:
    parsed = parse_periodic_table_page(
        RU_TABLE_HTML,
        language="ru",
        url="https://ru.wikipedia.org/wiki/Периодическая_система_химических_элементов",
    )

    assert parsed.title == "Периодическая система химических элементов"
    assert "атомному номеру" in (parsed.lead_summary or "")
    assert "схожими химическими свойствами" in (parsed.section_summaries["groups"] or "")
    assert len(parsed.candidate_urls) == 2
    assert parsed.candidate_urls[0].endswith("/wiki/%D0%92%D0%BE%D0%B4%D0%BE%D1%80%D0%BE%D0%B4")


def test_parse_element_page_maps_fields_sections_and_isotopes() -> None:
    parsed = parse_element_page(
        RU_HYDROGEN_HTML,
        language="ru",
        url="https://ru.wikipedia.org/wiki/Водород",
    )

    assert parsed is not None
    assert parsed["atomic_number"] == 1
    assert parsed["symbol"] == "H"
    assert parsed["classification"]["block"] == "s"
    assert parsed["properties"]["atomic_mass"]["value"] == 1.008
    assert parsed["properties"]["cas_number"] == "1333-74-0"
    assert parsed["sections"]["production"] is not None
    assert parsed["sections"]["production_home"] is not None
    assert parsed["sections"]["production_laboratory"] is not None
    assert parsed["sections"]["chemical_properties"]["summary"].startswith("Водород проявляет")
    assert parsed["isotopes"][0]["isotope"] == "1H"


def test_parse_element_page_supports_combined_live_style_rows() -> None:
    parsed = parse_element_page(
        LIVE_STYLE_RU_HTML,
        language="ru",
        url="https://ru.wikipedia.org/wiki/Водород",
    )

    assert parsed is not None
    assert parsed["atomic_number"] == 1
    assert parsed["symbol"] == "H"
    assert parsed["name"] == "Водород"
    assert parsed["classification"]["group"] == 1
    assert parsed["classification"]["period"] == 1
    assert parsed["classification"]["block"] == "s"


def test_merge_element_records_prefers_ru_and_falls_back_to_en() -> None:
    ru_record = parse_element_page(RU_HYDROGEN_HTML, language="ru", url="https://ru.wikipedia.org/wiki/Водород")
    en_record = parse_element_page(EN_HYDROGEN_HTML, language="en", url="https://en.wikipedia.org/wiki/Hydrogen")

    merged = merge_element_records(ru_record, en_record, "2026-03-22T00:00:00Z")

    assert merged["name_ru"] == "Водород"
    assert merged["name_en"] == "Hydrogen"
    assert merged["properties"]["atomic_mass"]["value"] == 1.008
    assert merged["properties"]["molar_mass"]["value"] == 1.008
    assert merged["properties"]["ionization_energy"]["value"] == 1312
    assert merged["sections"]["purification"]["summary"].startswith("Hydrogen can be purified")


def test_build_periodic_table_dataset_smoke_with_fake_fetch() -> None:
    pages = {
        "https://ru.wikipedia.org/wiki/Периодическая_система_химических_элементов": RU_TABLE_HTML,
        "https://en.wikipedia.org/wiki/Periodic_table": EN_TABLE_HTML,
        "https://ru.wikipedia.org/wiki/%D0%92%D0%BE%D0%B4%D0%BE%D1%80%D0%BE%D0%B4": RU_HYDROGEN_HTML,
        "https://ru.wikipedia.org/wiki/%D0%93%D0%B5%D0%BB%D0%B8%D0%B9": RU_HELIUM_HTML,
        "https://en.wikipedia.org/wiki/Hydrogen": EN_HYDROGEN_HTML,
        "https://en.wikipedia.org/wiki/Helium": EN_HELIUM_HTML,
    }

    def fake_fetch(url: str) -> str:
        return pages[url]

    dataset = build_periodic_table_dataset(
        fetch=fake_fetch,
        generated_at=datetime(2026, 3, 22, tzinfo=UTC),
    )

    assert dataset["meta"]["schema_version"] == "1.0.0"
    assert dataset["meta"]["element_count"] == 2
    assert [item["atomic_number"] for item in dataset["elements"]] == [1, 2]
    assert dataset["structure"]["element_count"] == 2
    assert dataset["groups"][0]["group"] == 1
    assert dataset["groups"][1]["group"] == 18
    assert dataset["periods"][0]["period"] == 1
    assert dataset["blocks"][0]["block"] == "s"
    assert dataset["elements"][0]["source_urls"]["ru"].endswith("/wiki/%D0%92%D0%BE%D0%B4%D0%BE%D1%80%D0%BE%D0%B4")
