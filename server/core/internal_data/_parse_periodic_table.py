from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag


SCHEMA_VERSION = "1.0.0"
USER_AGENT = "ASFES-MULTIPLEX periodic table builder/1.0 (+https://wikipedia.org)"
DEFAULT_TIMEOUT = 30
EXPECTED_ELEMENT_COUNT = 118
DEFAULT_OUTPUT_PATH = Path(__file__).with_name("periodic_table.json")
TABLE_PAGE_URLS = {
    "ru": "https://ru.wikipedia.org/wiki/Периодическая_система_химических_элементов",
    "en": "https://en.wikipedia.org/wiki/Periodic_table",
}
KNOWN_BLOCKS = ("s", "p", "d", "f")
KNOWN_SECTION_KEYS = (
    "production",
    "production_home",
    "production_laboratory",
    "purification",
    "chemical_properties",
    "physical_properties",
    "history",
    "occurrence",
    "applications",
    "biological_role",
    "safety",
)
SKIPPED_SECTION_LABELS = {
    "references",
    "notes",
    "external links",
    "see also",
    "literature",
    "примечания",
    "ссылки",
    "литература",
    "см. также",
    "источники",
}
STRUCTURE_SECTION_ALIASES = {
    "structure": {
        "строение таблицы",
        "structure",
        "organization",
        "организация",
        "arrangement",
        "расположение",
    },
    "groups": {
        "group",
        "groups",
        "группа",
        "группы",
    },
    "periods": {
        "period",
        "periods",
        "период",
        "периоды",
    },
    "blocks": {
        "block",
        "blocks",
        "блок",
        "блоки",
    },
}
SECTION_ALIASES = {
    "production": {
        "получение",
        "способы получения",
        "production",
        "preparation",
        "synthesis",
        "manufacture",
        "obtaining",
    },
    "production_home": {
        "получение в домашних условиях",
        "домашнее получение",
        "home preparation",
        "home production",
    },
    "production_laboratory": {
        "получение в лаборатории",
        "лабораторное получение",
        "laboratory preparation",
        "laboratory production",
        "lab preparation",
    },
    "purification": {
        "очистка",
        "рафинирование",
        "purification",
        "refining",
    },
    "chemical_properties": {
        "химические свойства",
        "chemical properties",
        "properties",
        "reactivity",
    },
    "physical_properties": {
        "физические свойства",
        "physical properties",
    },
    "history": {
        "история",
        "history",
        "этимология",
        "etymology",
    },
    "occurrence": {
        "нахождение в природе",
        "распространение",
        "occurrence",
        "natural occurrence",
        "abundance",
    },
    "applications": {
        "применение",
        "uses",
        "applications",
    },
    "biological_role": {
        "биологическая роль",
        "biological role",
    },
    "safety": {
        "меры предосторожности",
        "безопасность",
        "safety",
        "precautions",
    },
}
NUMERIC_PROPERTY_KEYS = {
    "atomic_mass",
    "molar_mass",
    "atomic_radius",
    "covalent_radius",
    "ionic_radius",
    "electronegativity",
    "ionization_energy",
    "density",
    "melting_point",
    "boiling_point",
    "triple_point",
    "critical_point",
    "heat_of_fusion",
    "heat_of_vaporization",
    "molar_heat_capacity",
    "molar_volume",
    "c_a_ratio",
    "thermal_conductivity",
}
PROPERTY_ALIASES = {
    "название": "name",
    "name": "name",
    "symbol": "symbol",
    "символ": "symbol",
    "atomic number": "atomic_number",
    "порядковый номер": "atomic_number",
    "атомный номер": "atomic_number",
    "group": "group",
    "группа": "group",
    "period": "period",
    "период": "period",
    "block": "block",
    "блок": "block",
    "category": "category",
    "chemical series": "category",
    "серия": "category",
    "категория": "category",
    "appearance": "appearance",
    "внешний вид": "appearance",
    "standard atomic weight": "atomic_mass",
    "atomic weight": "atomic_mass",
    "атомная масса": "atomic_mass",
    "atomic mass": "atomic_mass",
    "молярная масса": "molar_mass",
    "molar mass": "molar_mass",
    "electron configuration": "electron_configuration",
    "электронная конфигурация": "electron_configuration",
    "atomic radius": "atomic_radius",
    "радиус атома": "atomic_radius",
    "covalent radius": "covalent_radius",
    "ковалентный радиус": "covalent_radius",
    "van der waals radius": "van_der_waals_radius",
    "радиус ван дер ваальса": "van_der_waals_radius",
    "ionic radius": "ionic_radius",
    "радиус иона": "ionic_radius",
    "electronegativity": "electronegativity",
    "электроотрицательность": "electronegativity",
    "oxidation states": "oxidation_states",
    "степени окисления": "oxidation_states",
    "ionization energy": "ionization_energy",
    "energies of ionization": "ionization_energy",
    "энергия ионизации": "ionization_energy",
    "density": "density",
    "плотность": "density",
    "melting point": "melting_point",
    "температура плавления": "melting_point",
    "boiling point": "boiling_point",
    "температура кипения": "boiling_point",
    "triple point": "triple_point",
    "тройная точка": "triple_point",
    "critical point": "critical_point",
    "критическая точка": "critical_point",
    "heat of fusion": "heat_of_fusion",
    "molar heat of fusion": "heat_of_fusion",
    "мол теплота плавления": "heat_of_fusion",
    "мольная теплота плавления": "heat_of_fusion",
    "heat of vaporization": "heat_of_vaporization",
    "molar heat of vaporization": "heat_of_vaporization",
    "мол теплота испарения": "heat_of_vaporization",
    "мольная теплота испарения": "heat_of_vaporization",
    "molar heat capacity": "molar_heat_capacity",
    "молярная теплоемкость": "molar_heat_capacity",
    "молярная теплоёмкость": "molar_heat_capacity",
    "molar volume": "molar_volume",
    "молярный объем": "molar_volume",
    "молярный объём": "molar_volume",
    "crystal structure": "crystal_structure",
    "структура решетки": "crystal_structure",
    "структура решётки": "crystal_structure",
    "lattice constants": "lattice_parameters",
    "lattice parameters": "lattice_parameters",
    "параметры решетки": "lattice_parameters",
    "параметры решётки": "lattice_parameters",
    "c/a ratio": "c_a_ratio",
    "отношение c/a": "c_a_ratio",
    "thermal conductivity": "thermal_conductivity",
    "теплопроводность": "thermal_conductivity",
    "cas number": "cas_number",
    "номер cas": "cas_number",
    "cas": "cas_number",
    "most stable isotopes": "longest_lived_isotopes_text",
    "наиболее долгоживущие изотопы": "longest_lived_isotopes_text",
}


FetchFunction = Callable[[str], str]


@dataclass(slots=True)
class PageSummary:
    language: str
    url: str
    title: str | None
    lead_summary: str | None
    section_summaries: dict[str, str | None]
    candidate_urls: list[str]


def _clean_text(value: str) -> str:
    text = re.sub(r"\[[0-9]+\]", "", value or "")
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _normalize_label(value: str) -> str:
    text = _clean_text(value).lower()
    text = text.replace("ё", "е")
    text = text.replace("–", "-").replace("—", "-")
    text = re.sub(r"\([^)]*\)", "", text)
    text = re.sub(r"[:;]+$", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip(" -")


def _extract_sentences(text: str, limit: int = 2) -> str | None:
    cleaned = _clean_text(text)
    if not cleaned:
        return None
    parts = re.split(r"(?<=[.!?])\s+", cleaned)
    summary = " ".join(part for part in parts[:limit] if part)
    return summary or cleaned


def _tag_text(tag: Tag | None) -> str:
    if tag is None:
        return ""
    return _clean_text(tag.get_text(" ", strip=True))


def _extract_first_heading_text(soup: BeautifulSoup) -> str | None:
    heading = soup.select_one("#firstHeading")
    if heading:
        text = _tag_text(heading)
        return text or None
    title = soup.find("title")
    if title:
        text = _tag_text(title)
        if text:
            return text.split(" - ", 1)[0]
    return None


def _mw_parser_output(soup: BeautifulSoup) -> Tag:
    output = soup.select_one(".mw-parser-output")
    if output:
        return output
    body = soup.body
    if body:
        return body
    return soup


def _is_internal_wiki_link(href: str) -> bool:
    parsed = urlparse(href)
    path = parsed.path
    if not path.startswith("/wiki/"):
        return False
    if ":" in path:
        return False
    return True


def _lead_paragraphs(soup: BeautifulSoup, limit: int = 2) -> list[str]:
    output = _mw_parser_output(soup)
    paragraphs: list[str] = []
    for child in output.children:
        if not isinstance(child, Tag):
            continue
        if child.name == "p":
            text = _tag_text(child)
            if text:
                paragraphs.append(text)
            if len(paragraphs) >= limit:
                break
        elif child.name and child.name.startswith("h"):
            break
    return paragraphs


def _heading_title(heading: Tag) -> str:
    headline = heading.select_one(".mw-headline")
    if headline:
        return _tag_text(headline)
    return _tag_text(heading)


def _collect_section_blocks(soup: BeautifulSoup) -> list[dict[str, Any]]:
    output = _mw_parser_output(soup)
    blocks: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    for child in output.children:
        if not isinstance(child, Tag):
            continue
        if child.name in {"h2", "h3", "h4"}:
            title = _heading_title(child)
            normalized = _normalize_label(title)
            if normalized in SKIPPED_SECTION_LABELS:
                current = None
                continue
            current = {"title": title, "normalized_title": normalized, "chunks": []}
            blocks.append(current)
            continue
        if current is None:
            continue
        if child.name in {"p", "ul", "ol"}:
            text = _tag_text(child)
            if text:
                current["chunks"].append(text)
        elif child.name == "table":
            table_text = _tag_text(child)
            if table_text:
                current["chunks"].append(table_text)
    for block in blocks:
        raw_text = "\n".join(block["chunks"]).strip()
        block["raw_text"] = raw_text or None
        block["summary"] = _extract_sentences(raw_text, limit=2) if raw_text else None
    return blocks


def _canonical_section_key(title: str) -> str | None:
    normalized = _normalize_label(title)
    for key, aliases in SECTION_ALIASES.items():
        if normalized in aliases:
            return key
    return None


def _canonical_structure_key(title: str) -> str | None:
    normalized = _normalize_label(title)
    for key, aliases in STRUCTURE_SECTION_ALIASES.items():
        if normalized in aliases:
            return key
    return None


def _section_record(title: str, summary: str | None, raw_text: str | None, url: str) -> dict[str, Any]:
    return {
        "title": title,
        "summary": summary,
        "raw_text": raw_text,
        "url": url,
    }


def _detect_unit(raw_text: str, numeric_part: str) -> str | None:
    remainder = raw_text.replace(numeric_part, "", 1).strip(" ;,:")
    remainder = re.sub(r"^[=~≈±]+", "", remainder).strip()
    return remainder or None


def _parse_numeric_value(raw_text: str) -> dict[str, Any]:
    cleaned = _clean_text(raw_text)
    if not cleaned:
        return {"value": None, "unit": None, "raw": raw_text}
    if any(token in cleaned for token in ("–", "—", " to ", "±")):
        return {"value": None, "unit": None, "raw": cleaned}
    numbers = re.findall(r"[-+]?\d+(?:[.,]\d+)?", cleaned)
    if len(numbers) != 1:
        return {"value": None, "unit": None, "raw": cleaned}
    numeric_text = numbers[0].replace(",", ".")
    try:
        value = float(numeric_text)
    except ValueError:
        value = None
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    return {
        "value": value,
        "unit": _detect_unit(cleaned, numbers[0]),
        "raw": cleaned,
    }


def _normalize_property_value(key: str, raw_text: str) -> Any:
    if key in NUMERIC_PROPERTY_KEYS:
        return _parse_numeric_value(raw_text)
    if key in {"atomic_number", "group", "period"}:
        match = re.search(r"\b([1-9]\d?|1[01]\d|118)\b", raw_text)
        return int(match.group(1)) if match else None
    if key == "block":
        match = re.search(r"\b([spdf])\b", raw_text.lower())
        return match.group(1) if match else _clean_text(raw_text) or None
    if key == "symbol":
        text = _clean_text(raw_text)
        match = re.search(r"\b([A-Z][a-z]?)\b", text)
        return match.group(1) if match else text or None
    if key == "cas_number":
        text = _clean_text(raw_text)
        match = re.search(r"\b\d{2,7}-\d{2}-\d\b", text)
        return match.group(0) if match else text or None
    text = _clean_text(raw_text)
    return text or None


def _remember_field(
    raw_fields: dict[str, list[dict[str, Any]]],
    canonical_values: dict[str, Any],
    *,
    key: str,
    label: str,
    raw_value: str,
    parsed_value: Any,
) -> None:
    raw_fields[key].append({"label": label, "raw": raw_value, "parsed": parsed_value})
    if canonical_values.get(key) in (None, "", {}, []):
        canonical_values[key] = parsed_value


def _parse_combined_row(label: str, value: str, raw_fields: dict[str, list[dict[str, Any]]], canonical_values: dict[str, Any]) -> bool:
    normalized = _normalize_label(label)
    handled = False
    if ("название" in normalized and "символ" in normalized and "номер" in normalized) or (
        "name" in normalized and "symbol" in normalized and "number" in normalized
    ):
        cleaned_value = _clean_text(value)
        atomic_match = re.search(r"(\d{1,3})\s*$", cleaned_value)
        symbol_match = re.search(r"\(([A-Z][a-z]?)\)", cleaned_value)
        name_part = cleaned_value.split(",", 1)[0]
        name_part = name_part.split("/", 1)[0].strip()
        if name_part:
            _remember_field(raw_fields, canonical_values, key="name", label=label, raw_value=value, parsed_value=name_part)
        if symbol_match:
            _remember_field(raw_fields, canonical_values, key="symbol", label=label, raw_value=value, parsed_value=symbol_match.group(1))
        if atomic_match:
            _remember_field(
                raw_fields,
                canonical_values,
                key="atomic_number",
                label=label,
                raw_value=value,
                parsed_value=int(atomic_match.group(1)),
            )
        handled = True
    if ("группа" in normalized and "период" in normalized and "блок" in normalized) or (
        "group" in normalized and "period" in normalized and "block" in normalized
    ):
        numbers = re.findall(r"\b([1-9]\d?|1[01]\d|118)\b", value)
        block_match = re.search(r"\b([spdf])(?:-|\s)?(?:element|элемент|block)?\b", value.lower())
        if numbers:
            _remember_field(raw_fields, canonical_values, key="group", label=label, raw_value=value, parsed_value=int(numbers[0]))
        if len(numbers) > 1:
            _remember_field(raw_fields, canonical_values, key="period", label=label, raw_value=value, parsed_value=int(numbers[1]))
        if block_match:
            _remember_field(raw_fields, canonical_values, key="block", label=label, raw_value=value, parsed_value=block_match.group(1))
        handled = True
    return handled


def _candidate_element_urls(soup: BeautifulSoup, base_url: str) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()
    for cell in soup.select("table td, table th"):
        cell_text = _tag_text(cell)
        has_atomic_number = re.search(r"\b([1-9]\d?|1[01]\d|118)\b", cell_text) is not None
        links = cell.select("a[href]")
        if not links:
            continue
        if not has_atomic_number and len(cell_text) > 80:
            continue
        for anchor in links:
            href = anchor.get("href", "")
            if not _is_internal_wiki_link(href):
                continue
            candidate = urljoin(base_url, href)
            if candidate in seen:
                continue
            text = _tag_text(anchor)
            if len(text) > 40:
                continue
            seen.add(candidate)
            candidates.append(candidate)
    if len(candidates) >= EXPECTED_ELEMENT_COUNT:
        return candidates
    for anchor in soup.select("table a[href]"):
        href = anchor.get("href", "")
        if not _is_internal_wiki_link(href):
            continue
        candidate = urljoin(base_url, href)
        if candidate in seen:
            continue
        seen.add(candidate)
        candidates.append(candidate)
    return candidates


def parse_periodic_table_page(html: str, *, language: str, url: str) -> PageSummary:
    soup = BeautifulSoup(html, "html.parser")
    sections = _collect_section_blocks(soup)
    section_summaries = {key: None for key in STRUCTURE_SECTION_ALIASES}
    for block in sections:
        key = _canonical_structure_key(block["title"])
        if key and not section_summaries[key]:
            section_summaries[key] = block["summary"]
    lead_summary = _extract_sentences(" ".join(_lead_paragraphs(soup)), limit=2)
    return PageSummary(
        language=language,
        url=url,
        title=_extract_first_heading_text(soup),
        lead_summary=lead_summary,
        section_summaries=section_summaries,
        candidate_urls=_candidate_element_urls(soup, url),
    )


def _parse_isotope_table(container: Tag | None) -> list[dict[str, Any]]:
    if container is None:
        return []
    isotopes: list[dict[str, Any]] = []
    for row in container.select("tr"):
        cells = row.find_all(["th", "td"])
        if len(cells) < 2:
            continue
        values = [_tag_text(cell) for cell in cells]
        if not any(values):
            continue
        if any("изотоп" in value.lower() or "isotope" in value.lower() for value in values[:2]):
            continue
        isotopes.append(
            {
                "isotope": values[0],
                "details": values[1:],
                "raw": " | ".join(value for value in values if value),
            }
        )
        if len(isotopes) >= 10:
            break
    return isotopes


def _best_symbol_from_title(title: str | None) -> str | None:
    if not title:
        return None
    match = re.fullmatch(r"[A-Z][a-z]?", title.strip())
    if match:
        return match.group(0)
    return None


def parse_element_page(html: str, *, language: str, url: str) -> dict[str, Any] | None:
    soup = BeautifulSoup(html, "html.parser")
    title = _extract_first_heading_text(soup)
    infobox = soup.select_one("table.infobox")
    if infobox is None:
        return None

    raw_fields: dict[str, list[dict[str, Any]]] = defaultdict(list)
    canonical_values: dict[str, Any] = {"name": title}
    for row in infobox.select("tr"):
        headers = row.find_all("th", recursive=False)
        values = row.find_all("td", recursive=False)
        if not headers or not values:
            continue
        label = _tag_text(headers[0])
        value = _tag_text(values[0])
        if not label or not value:
            continue
        if _parse_combined_row(label, value, raw_fields, canonical_values):
            continue
        canonical_key = PROPERTY_ALIASES.get(_normalize_label(label))
        if canonical_key is None:
            continue
        parsed_value = _normalize_property_value(canonical_key, value)
        _remember_field(raw_fields, canonical_values, key=canonical_key, label=label, raw_value=value, parsed_value=parsed_value)

    atomic_number = canonical_values.get("atomic_number")
    if not isinstance(atomic_number, int) or not (1 <= atomic_number <= EXPECTED_ELEMENT_COUNT):
        return None

    symbol = canonical_values.get("symbol") or _best_symbol_from_title(title)
    sections = {key: None for key in KNOWN_SECTION_KEYS}
    sections["additional_sections"] = []
    isotopes: list[dict[str, Any]] = []

    heading_lookup = {
        _heading_title(tag): tag
        for tag in soup.select(".mw-parser-output h2, .mw-parser-output h3, .mw-parser-output h4")
    }
    for block in _collect_section_blocks(soup):
        title_text = block["title"]
        raw_text = block["raw_text"]
        summary = block["summary"]
        canonical_key = _canonical_section_key(title_text)
        record = _section_record(title_text, summary, raw_text, url)
        lowered = _normalize_label(title_text)
        if "изотоп" in lowered or "isotope" in lowered:
            heading_tag = heading_lookup.get(title_text)
            next_table = heading_tag.find_next("table") if heading_tag else None
            isotopes = isotopes or _parse_isotope_table(next_table)
        if canonical_key:
            sections[canonical_key] = record
            if canonical_key == "production":
                raw_lower = (raw_text or "").lower()
                if any(token in raw_lower for token in ("домаш", "home")) and sections["production_home"] is None:
                    sections["production_home"] = record
                if any(token in raw_lower for token in ("лаборатор", "laborator", "lab ")) and sections["production_laboratory"] is None:
                    sections["production_laboratory"] = record
        elif raw_text:
            sections["additional_sections"].append(record)

    if not isotopes and raw_fields.get("longest_lived_isotopes_text"):
        raw_value = raw_fields["longest_lived_isotopes_text"][0]["raw"]
        isotopes = [{"isotope": raw_value, "details": [], "raw": raw_value}]

    properties: dict[str, Any] = {}
    for key, value in canonical_values.items():
        if key in {"atomic_number", "symbol", "group", "period", "block", "name", "category"}:
            continue
        properties[key] = value

    return {
        "atomic_number": atomic_number,
        "symbol": symbol,
        "name": canonical_values.get("name") or title,
        "classification": {
            "group": canonical_values.get("group"),
            "period": canonical_values.get("period"),
            "block": canonical_values.get("block"),
            "category": canonical_values.get("category"),
        },
        "properties": properties,
        "isotopes": isotopes,
        "sections": sections,
        "source": {
            "language": language,
            "url": url,
            "page_title": title,
            "field_values": dict(raw_fields),
        },
    }


class WikipediaFetcher:
    def __init__(self, timeout: int = DEFAULT_TIMEOUT) -> None:
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})

    def fetch(self, url: str) -> str:
        response = self.session.get(url, timeout=self.timeout)
        response.raise_for_status()
        return response.text


def _prefer_primary(primary: Any, fallback: Any) -> Any:
    if primary not in (None, "", {}, []):
        return primary
    return fallback


def _merge_property_values(primary: Any, fallback: Any) -> Any:
    if isinstance(primary, dict) and "raw" in primary:
        if primary.get("value") is not None or primary.get("raw"):
            return primary
    if primary not in (None, "", {}, []):
        return primary
    return fallback


def _merge_section_records(primary: dict[str, Any] | None, fallback: dict[str, Any] | None) -> dict[str, Any] | None:
    return primary or fallback


def merge_element_records(ru_record: dict[str, Any] | None, en_record: dict[str, Any] | None, generated_at: str) -> dict[str, Any]:
    source = ru_record or en_record
    if source is None:
        raise ValueError("At least one source record is required")

    atomic_number = source["atomic_number"]
    symbol = _prefer_primary(ru_record.get("symbol") if ru_record else None, en_record.get("symbol") if en_record else None)
    classification = {
        "group": _prefer_primary(
            ru_record["classification"].get("group") if ru_record else None,
            en_record["classification"].get("group") if en_record else None,
        ),
        "period": _prefer_primary(
            ru_record["classification"].get("period") if ru_record else None,
            en_record["classification"].get("period") if en_record else None,
        ),
        "block": _prefer_primary(
            ru_record["classification"].get("block") if ru_record else None,
            en_record["classification"].get("block") if en_record else None,
        ),
        "category": _prefer_primary(
            ru_record["classification"].get("category") if ru_record else None,
            en_record["classification"].get("category") if en_record else None,
        ),
    }

    property_keys = set()
    if ru_record:
        property_keys.update(ru_record["properties"].keys())
    if en_record:
        property_keys.update(en_record["properties"].keys())
    properties = {
        key: _merge_property_values(
            ru_record["properties"].get(key) if ru_record else None,
            en_record["properties"].get(key) if en_record else None,
        )
        for key in sorted(property_keys)
    }

    sections: dict[str, Any] = {}
    for key in KNOWN_SECTION_KEYS:
        sections[key] = _merge_section_records(
            ru_record["sections"].get(key) if ru_record else None,
            en_record["sections"].get(key) if en_record else None,
        )
    sections["additional_sections"] = []
    seen_additional: set[tuple[str, str]] = set()
    for record in (ru_record, en_record):
        if record is None:
            continue
        for item in record["sections"].get("additional_sections", []):
            marker = (item.get("title") or "", item.get("url") or "")
            if marker in seen_additional:
                continue
            seen_additional.add(marker)
            sections["additional_sections"].append(item)

    isotopes = ru_record["isotopes"] if ru_record and ru_record["isotopes"] else en_record["isotopes"] if en_record else []

    return {
        "atomic_number": atomic_number,
        "symbol": symbol,
        "name_ru": ru_record.get("name") if ru_record else None,
        "name_en": en_record.get("name") if en_record else None,
        "source_urls": {
            "ru": ru_record["source"]["url"] if ru_record else None,
            "en": en_record["source"]["url"] if en_record else None,
        },
        "identity": {
            "atomic_number": atomic_number,
            "symbol": symbol,
            "name_ru": ru_record.get("name") if ru_record else None,
            "name_en": en_record.get("name") if en_record else None,
            "aliases": [
                name
                for name in [
                    ru_record.get("name") if ru_record else None,
                    en_record.get("name") if en_record else None,
                ]
                if name
            ],
            "source_urls": {
                "ru": ru_record["source"]["url"] if ru_record else None,
                "en": en_record["source"]["url"] if en_record else None,
            },
        },
        "classification": classification,
        "properties": properties,
        "isotopes": isotopes,
        "sections": sections,
        "sources": {
            "generated_at": generated_at,
            "languages": {
                "ru": ru_record["source"] if ru_record else None,
                "en": en_record["source"] if en_record else None,
            },
        },
    }


def _completeness_score(record: dict[str, Any]) -> int:
    score = 0
    score += sum(1 for value in record.get("properties", {}).values() if value not in (None, "", {}, []))
    score += sum(1 for value in record.get("classification", {}).values() if value not in (None, "", {}, []))
    score += len(record.get("isotopes", []))
    return score


def _collect_language_elements(candidate_urls: list[str], *, language: str, fetch: FetchFunction) -> dict[int, dict[str, Any]]:
    elements: dict[int, dict[str, Any]] = {}
    for url in candidate_urls:
        try:
            html = fetch(url)
        except Exception:
            continue
        parsed = parse_element_page(html, language=language, url=url)
        if parsed is None:
            continue
        existing = elements.get(parsed["atomic_number"])
        if existing is None or _completeness_score(parsed) > _completeness_score(existing):
            elements[parsed["atomic_number"]] = parsed
    return elements


def _field_coverage(elements: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    total = len(elements) or 1
    counter: defaultdict[str, int] = defaultdict(int)
    for element in elements:
        for key, value in element["properties"].items():
            if value in (None, "", {}, []):
                continue
            if isinstance(value, dict) and "raw" in value and value.get("value") is None and not value.get("raw"):
                continue
            counter[key] += 1
        for key in ("group", "period", "block", "category"):
            if element["classification"].get(key) not in (None, "", {}, []):
                counter[f"classification.{key}"] += 1
        if element.get("isotopes"):
            counter["isotopes"] += 1
        for key in KNOWN_SECTION_KEYS:
            if element["sections"].get(key):
                counter[f"sections.{key}"] += 1
    return {
        key: {"count": value, "ratio": round(value / total, 4)}
        for key, value in sorted(counter.items())
    }


def _summarize_structure(
    elements: list[dict[str, Any]],
    ru_page: PageSummary,
    en_page: PageSummary,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    groups_map: defaultdict[int, list[int]] = defaultdict(list)
    periods_map: defaultdict[int, list[int]] = defaultdict(list)
    blocks_map: defaultdict[str, list[int]] = defaultdict(list)
    categories: set[str] = set()

    for element in elements:
        atomic_number = element["atomic_number"]
        classification = element["classification"]
        group = classification.get("group")
        period = classification.get("period")
        block = classification.get("block")
        category = classification.get("category")
        if isinstance(group, int):
            groups_map[group].append(atomic_number)
        if isinstance(period, int):
            periods_map[period].append(atomic_number)
        if isinstance(block, str):
            blocks_map[block].append(atomic_number)
        if category:
            categories.add(str(category))

    structure = {
        "summary": ru_page.lead_summary or en_page.lead_summary,
        "summary_en": en_page.lead_summary,
        "title_ru": ru_page.title,
        "title_en": en_page.title,
        "element_count": len(elements),
        "source_urls": {"ru": ru_page.url, "en": en_page.url},
        "section_summaries": {
            "structure": ru_page.section_summaries.get("structure") or en_page.section_summaries.get("structure"),
            "groups": ru_page.section_summaries.get("groups") or en_page.section_summaries.get("groups"),
            "periods": ru_page.section_summaries.get("periods") or en_page.section_summaries.get("periods"),
            "blocks": ru_page.section_summaries.get("blocks") or en_page.section_summaries.get("blocks"),
        },
        "legend": {
            "categories": sorted(categories),
            "blocks": [block for block in KNOWN_BLOCKS if block in blocks_map],
        },
    }

    groups = [
        {
            "group": group,
            "description": f"Группа {group} периодической таблицы; элементов в датасете: {len(sorted(numbers))}.",
            "element_numbers": sorted(numbers),
        }
        for group, numbers in sorted(groups_map.items())
    ]
    periods = [
        {
            "period": period,
            "description": f"Период {period} периодической таблицы; элементов в датасете: {len(sorted(numbers))}.",
            "element_numbers": sorted(numbers),
        }
        for period, numbers in sorted(periods_map.items())
    ]
    blocks = [
        {
            "block": block,
            "description": f"{block}-блок периодической таблицы; элементов в датасете: {len(sorted(numbers))}.",
            "element_numbers": sorted(numbers),
        }
        for block, numbers in sorted(blocks_map.items())
    ]
    return structure, groups, periods, blocks


def build_periodic_table_dataset(
    *,
    fetch: FetchFunction | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    if fetch is None:
        fetcher = WikipediaFetcher()
        fetch = fetcher.fetch

    now = generated_at or datetime.now(UTC)
    generated_at_iso = now.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    table_pages: dict[str, PageSummary] = {}
    elements_by_language: dict[str, dict[int, dict[str, Any]]] = {}
    errors: list[dict[str, str]] = []

    for language, url in TABLE_PAGE_URLS.items():
        try:
            html = fetch(url)
        except Exception as exc:
            errors.append({"scope": "table_page", "language": language, "url": url, "error": str(exc)})
            continue
        page_summary = parse_periodic_table_page(html, language=language, url=url)
        table_pages[language] = page_summary
        elements_by_language[language] = _collect_language_elements(page_summary.candidate_urls, language=language, fetch=fetch)

    ru_page = table_pages.get("ru") or PageSummary("ru", TABLE_PAGE_URLS["ru"], None, None, {key: None for key in STRUCTURE_SECTION_ALIASES}, [])
    en_page = table_pages.get("en") or PageSummary("en", TABLE_PAGE_URLS["en"], None, None, {key: None for key in STRUCTURE_SECTION_ALIASES}, [])
    ru_elements = elements_by_language.get("ru", {})
    en_elements = elements_by_language.get("en", {})
    atomic_numbers = sorted(set(ru_elements) | set(en_elements))

    elements = [
        merge_element_records(ru_elements.get(number), en_elements.get(number), generated_at_iso)
        for number in atomic_numbers
    ]
    elements.sort(key=lambda item: item["atomic_number"])

    structure, groups, periods, blocks = _summarize_structure(elements, ru_page, en_page)
    return {
        "meta": {
            "schema_version": SCHEMA_VERSION,
            "generated_at": generated_at_iso,
            "sources": TABLE_PAGE_URLS,
            "element_count": len(elements),
            "expected_element_count": EXPECTED_ELEMENT_COUNT,
            "coverage": _field_coverage(elements),
            "errors": errors,
        },
        "structure": structure,
        "groups": groups,
        "periods": periods,
        "blocks": blocks,
        "elements": elements,
    }


def write_periodic_table_json(dataset: dict[str, Any], output_path: Path = DEFAULT_OUTPUT_PATH) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    temp_path.write_text(json.dumps(dataset, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(output_path)
    return output_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Build an offline periodic table dataset from RU/EN Wikipedia.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH, help="Target JSON path.")
    args = parser.parse_args()

    dataset = build_periodic_table_dataset()
    write_periodic_table_json(dataset, args.output)
    print(f"Wrote periodic table dataset with {dataset['meta']['element_count']} elements to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
