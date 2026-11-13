from spain_electricity import catalog, charts, views


def test_every_shown_series_is_explained():
    shown = [column for column in views.HISTORY_COLUMNS if column != "hour_utc"] + list(
        charts.VARIABLE_LABELS
    )
    for key in shown:
        assert catalog.entry(key).description


def test_every_mix_technology_is_explained_in_slot_order():
    assert [entry.key for entry in catalog.TECHNOLOGIES] == list(charts.TECHNOLOGY_LABELS)
    assert set(charts.TECHNOLOGY_LABELS) == set(views.MIX_TECHNOLOGIES)


def test_every_dataset_names_its_source():
    assert set(catalog.DATASETS) == set(views.DATASETS)


def test_external_sources_link_over_https():
    for source in catalog.SOURCES:
        assert source.url is None or source.url.startswith("https://")


def test_markdown_lists_label_unit_and_link():
    text = catalog.markdown([catalog.entry("gas_price_eur_mwh")])
    assert text.startswith("- **Gas price** (€/MWh):")
    assert "(https://www.mibgas.es/en/file-access)" in text


def test_attribute_describes_technology_columns_with_their_entry():
    hydro = catalog.entry("hydro")
    assert catalog.attribute("hydro_mw") == (hydro.unit, hydro.description)


def test_attribute_describes_structural_columns():
    unit, description = catalog.attribute("ts_utc")
    assert unit == "" and "UTC" in description


def test_attribute_of_an_unknown_column_is_blank():
    assert catalog.attribute("not_a_column") == ("", "")
