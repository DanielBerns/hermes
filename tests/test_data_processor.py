import pytest

from hermes.domain.data_processor import (
    DataProcessor,
    DataProcessorException,
    get_city_key,
    price_to_cents
)

# --- Tests for Utility Functions ---

def test_get_city_key():
    assert get_city_key("chubut", "comodoro rivadavia") == "(chubut)[comodoro rivadavia]"

def test_price_to_cents():
    assert price_to_cents("150.75") == 15075
    assert price_to_cents("12.00") == 1200
    assert price_to_cents("75") == 7500

def test_price_to_cents_invalid_input():
    with pytest.raises(DataProcessorException):
        price_to_cents("not a price")
    with pytest.raises(DataProcessorException):
        price_to_cents(None)

# --- Tests for DataProcessor Class ---

@pytest.fixture
def processor() -> DataProcessor:
    """Provides a fresh DataProcessor instance for each test."""
    return DataProcessor()

def test_process_point_of_sale(processor: DataProcessor):
    """Tests the successful transformation of a point-of-sale row."""
    raw_data = {
        "id": " 1-1-1 ",
        "provincia": " AR-B",
        "localidad": "La Plata ",
        "direccion": "Calle 1",
        "banderaDescripcion": "Disco",
        "comercioRazonSocial": "Cencosud",
        "sucursalNombre": "Disco 1"
    }

    processed = processor.process_point_of_sale(raw_data)

    assert processed["point_of_sale_code"] == "1-1-1"
    assert processed["state"] == "ar-b"
    assert processed["city"] == "la plata"
    assert processed["flag"] == "disco"
    assert processed["point_of_sale_key"] == "(1-1-1)(disco)"
    assert processor._pos_code_to_key_map["1-1-1"] == "(1-1-1)(disco)"

def test_process_article(processor: DataProcessor):
    """Tests the successful transformation of an article row."""
    # First, process a POS to populate the internal map
    pos_raw = {
        "id": "1-1-1", "provincia": "AR-B", "localidad": "La Plata",
        "direccion": "Calle 1", "banderaDescripcion": "Disco",
        "comercioRazonSocial": "Cencosud", "sucursalNombre": "Disco 1"
    }
    processor.process_point_of_sale(pos_raw)

    article_raw = {
        "id": " 779001 ",
        "marca": "Coca-Cola",
        "nombre": " Gaseosa ",
        "presentacion": "2.25L",
        "precio": "150.00",
        "point_of_sale_id": "1-1-1"
    }

    processed = processor.process_article(article_raw)

    assert processed["article_code"] == "779001"
    assert processed["brand"] == "coca-cola"
    assert processed["price"] == 15000
    assert processed["point_of_sale_key"] == "(1-1-1)(disco)"
    assert processed["article_card_key"] == "[779001](coca-cola)(gaseosa)(2.25l)"

def test_process_article_with_unknown_pos(processor: DataProcessor):
    """Tests that processing an article fails if its point of sale wasn't processed first."""
    article_raw = {"point_of_sale_id": "unknown-pos", "id": "1", "marca": "a", "nombre": "b", "presentacion": "c", "precio": "1"}

    with pytest.raises(DataProcessorException, match="Unknown point_of_sale_code"):
        processor.process_article(article_raw)

def test_missing_key_in_pos_row(processor: DataProcessor):
    """Tests that a KeyError raises a DataProcessorException for POS rows."""
    incomplete_row = {"id": "1-1-1"} # Missing 'provincia' and others
    with pytest.raises(DataProcessorException, match="Missing expected key"):
        processor.process_point_of_sale(incomplete_row)

