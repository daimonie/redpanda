import unittest
import pandas as pd
import sys
import pytest
from unittest import mock
from tests.mock_lib import MagicAlchemySchema, MagicAttribute, MagicAlchemy
from app.sql.queries import general
from unittest.mock import Mock, MagicMock
from sqlalchemy.exc import DBAPIError


# This is our mocked "database" - the schemas of the tables.
model_entity = MagicAlchemySchema(
    "app.sql.models.entity",
    [
        "cookie",
        "ingredient",
        "date_baked",
        "province"
    ]
)
model_entitycode = MagicAlchemySchema(
    "app.sql.models.entitycode",
    [
        "ingredient",
        "ingredient_allergies"
    ]
)


def database_error():
    """Will be used as a callback function so we can simulate
    database failure"""
    raise DBAPIError("", "", "")


@mock.patch("app.sql.models.entity", model_entity)
@mock.patch("app.sql.models.entitycode", model_entitycode)
class GeneralTestCase(unittest.TestCase):
    """Test cases for the app.sql.queries.general module"""

    def test_get_cookies(self):
        fake_db = MagicAlchemy(name="db")

        general.get_cookies(fake_db)

        fake_db.test_calls("query")

    def test_get_cookies_database_error(self):
        fake_db = MagicAlchemy(name="db")

        fake_db.add_callback("query", database_error)

        # It is fine if it just raises the error, FastAPI has
        # an error handler. We just want to make sure it actually
        # does fail
        with pytest.raises(DBAPIError):
            general.get_cookies(fake_db)

    def test_get_cookies_per_sector(self):
        fake_db = MagicAlchemy(name="db")

        general.get_cookies_per_sector(
            fake_db,
            "fake_sector"
        )
        fake_db.test_calls("query")

        # with this right hand side?
        model_entity.ingredient.assert_called_with(
            "anything",
            "fake_sector"
        )

    def test_get_cookies_per_sector_database_error(self):
        fake_db = MagicAlchemy(name="db")
        fake_db.add_callback("query", database_error)

        # It is fine if it just raises the error, FastAPI has
        # an error handler. We just want to make sure it actually
        # does fail
        with pytest.raises(DBAPIError):
            general.get_cookies_per_sector(
                fake_db,
                "fake_sector"
            )

    def test_get_sectors(self):
        fake_db = MagicAlchemy(name="db")

        general.get_sectors(
            fake_db
        )
        fake_db.test_calls("query")

    def test_get_sectors_database_error(self):
        fake_db = MagicAlchemy(name="db")
        fake_db.add_callback("query", database_error)

        # It is fine if it just raises the error, FastAPI has
        # an error handler. We just want to make sure it actually
        # does fail
        with pytest.raises(DBAPIError):
            general.get_sectors(
                fake_db
            )

    def test_get_provinces(self):
        fake_db = MagicAlchemy(name="db")

        general.get_provinces(
            fake_db
        )
        fake_db.test_calls("query")

    def test_get_provinces_database_error(self):
        fake_db = MagicAlchemy(name="db")
        fake_db.add_callback("query", database_error)

        # It is fine if it just raises the error, FastAPI has
        # an error handler. We just want to make sure it actually
        # does fail
        with pytest.raises(DBAPIError):
            general.get_provinces(
                fake_db
            )
