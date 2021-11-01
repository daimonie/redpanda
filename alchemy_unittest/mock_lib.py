import unittest
import pandas as pd
import sys
from unittest import mock
from unittest.mock import Mock, MagicMock


class MagicAlchemy(MagicMock):
    """ This class mocks the SQLAlchemy database object.

        The normal SQLAlchemy object allows lazy loading and
        daisy-chaining, so that db.query(...).filter(...) is just
        as valid as db.filter(...).query(...). In our use case, we
        want to test a few things:
        - Whether or not you called a daisy-chained function, such as
            filter() or query()
        - Whether the lazy execution things parse, so for example a filter
            .filter( schema.date == today) should verify that the date field
            is set in the schema.

        The object implementation below will overwrite some of the MagicMock
        functionalities so that it is possible to count specific functions and
        daisy-chain with those specific functions. If you use another function,
        it will return a MagicMock object. So suppose wildcard is the function
        you called, you will get a MagicMock with name db.wildcard object and
        everything downstream from that will continue to be a simple MagicMock.

    """

    def __setattr__(self, name, value):
        """When you are setting a property that is used for the testing,
            we will set it. Otherwise default to the MagicMock functionality.
        """
        _reserved_names = [
            "_magic_alchemy_calls",
            "_callbacks"
        ]
        if name in _reserved_names:
            return object.__setattr__(self, name, value)

        return super(MagicAlchemy, self).__setattr__(name, value)

    def __init__(self, *args, **kwargs):
        """Set the private properties of MagicAlchemy and otherwise default to
        MagicMock"""
        self._magic_alchemy_calls = {}
        self._callbacks = {}

        super(MagicAlchemy, self).__init__(*args, **kwargs)

    def add_callback(self, key, func):
        """This allows you to return values or mocked data, or throw errors"""
        self._callbacks[key] = func

    def _callback(self, key):
        """This calls the callback if set."""
        if key in self._callbacks:
            self._callbacks[key]()

    def _add_call(self, key):
        """When a method is called, we count its calls and call the callback"""

        if key in self._magic_alchemy_calls:
            self._magic_alchemy_calls[key] += 1
        else:
            self._magic_alchemy_calls[key] = 1
        self._callback(key)

    def test_calls(self, key, number=1):
        """Test if a method is called, at least number times
        (default 1)"""
        assert key in self._magic_alchemy_calls
        assert self._magic_alchemy_calls[key] >= number

    # The following list are the supported daisy-chain methods for
    # database object.
    def filter(self, *args, **kwargs):
        self._add_call("filter")
        return self

    def query(self, *args, **kwargs):
        self._add_call("query")
        return self

    def all(self, *args, **kwargs):
        self._add_call("all")
        return self

    def group_by(self, *args, **kwargs):
        self._add_call("group_by")
        return self

    def order_by(self, *args, **kwargs):
        self._add_call("order_by")
        return self

    def join(self, *args, **kwargs):
        self._add_call("join")
        return self

    def limit(self, *args, **kwargs):
        self._add_call("limit")
        return self


class MagicAttribute(object):
    """ This class represents the field (name) of a SqlALchemy Base schema
    object. It is meant to completely overwrite the schema objects using
    a mock.

    When using it, each call or set or comparison to a schema field is tracked,
    allowing
    us to test:
    - Whether or not that is a legal field, using the initiated fields fo
        this database model
    - Whether or not that field has actually been used.

    """

    def __init__(self, value, name="unknown"):
        """The name is arbitrary but helps with debugging.
        The value is the original assigned value for this
        attribute."""
        self.name = name
        self.value = value
        self.operator_calls = {
            "lt": [],
            "gt": [],
            "le": [],
            "ge": [],
            "eq": [],
            "ne": []
        }

    def label(self, label_name):
        """This is equivalent to `field AS other_name` in SQL.
        It is used by the SQLAlchemy API, we just mock it and
        run away."""
        return MagicMock(name=label_name)

    # The following methods are comparison operators.
    #   This will just save (using operator_calls) whether
    #   or not it was called. It also keeps track of the values,
    #   so you can check if something was compared with something
    #   specific.
    def __lt__(self, other):
        self.operator_calls["lt"].append(other)
        return self.value.__lt__(other)

    def __gt__(self, other):
        self.operator_calls["gt"].append(other)
        return self.value.__gt__(other)

    def __le__(self, other):
        self.operator_calls["le"].append(other)
        return self.value.__le__(other)

    def __ge__(self, other):
        self.operator_calls["ge"].append(other)
        return self.value.__ge__(other)

    def __eq__(self, other):
        self.operator_calls["eq"].append(other)
        return self.value.__eq__(other)

    def __ne__(self, other):
        self.operator_calls["ne"].append(other)
        return self.value.__ne__(other)

    def assert_called(self, operator, number=1):
        """ Will check if the operator (or anything) was called
            more than number times (default once).
        """
        if operator == "anything":
            total_calls = sum([len(_) for _ in self.operator_calls])
            assert total_calls >= number
        else:
            assert len(self.operator_calls[operator]) >= number

    def assert_called_with(self, operator, other, number=1):
        """ Will check if the operator (or anything) was called
            more than number times (default once) but only when
            comparing with a specific value, other.
        """
        count = 0

        all_values = []
        if operator == "anything":
            for keys, values in self.operator_calls.items():
                count += 1 if other in values else 0
                all_values.append(values)
        else:
            values = self.operator_calls[operator]
            count += 1 if other in values else 0
            all_values.append(values)

        if count < number:
            raise AssertionError(
                f"Field {self.name} comparison [{operator}]"
                f" {other}  exactly {count} times, not {number} times."
            )


class MagicAlchemySchema(MagicMock):
    _magic_attr_calls = {}
    _magic_attr_values = {}

    def __init__(self, *args, **kwargs):
        """ This is to overwrite (mock) the SQLAlchemy schema objects,
            using also the MagicAttributes.

            It allows for more args/kwargs because it is a MagicMock derived
            class. The required arguments are a name and a string (list of)
            for the field names. Note that if you supply a string it will just
            parse a list.

            MagicAlchemySchema will use MagicAttributes to keep
            track of comparison counts, and otherwise make sure that calls to
            an attribute are counted (get and set).

        """
        assert len(args) >= 2

        name = args[0]
        allowed_fields = args[1]

        self._schema = name

        if not isinstance(allowed_fields, list):
            allowed_fields = [allowed_fields]

        self._relevant_attrs = allowed_fields

        self._magic_attr_calls = {name: 0 for name in self._relevant_attrs}
        self._magic_attr_values = {name: MagicAttribute(
            None, name=name) for name in self._relevant_attrs}

        super(MagicAlchemySchema, self).__init__(*args, **kwargs)

    def _reserved_names(self, sql_alchemy_schema_only=False):
        """These are names of this object and its parent, the MagicMock class.

        All other names can be assumed to be fields and are then counted when
        called."""
        # can't use dir(self) because it recurses infinitely
        sql_alchemy_schema_reserved = [
            "_schema",
            "_relevant_attrs",
            "_magic_attr_calls",
            "_magic_attr_values",
            "test_calls",
        ]
        if sql_alchemy_schema_only:
            return sql_alchemy_schema_reserved

        magic_mock_reserved = dir(MagicMock)

        # These were not captured by the dir trick
        extras_reserved = [
            "_mock_methods",
            "_mock_delegate",
            "_spec_class",
            "_mock_name",
            "_mock_new_name",
            "_mock_parent",
            "_mock_new_parent",
            "_spec_set"
        ]
        all_reserved = list(set(
            sql_alchemy_schema_reserved + magic_mock_reserved + extras_reserved
        ))

        return all_reserved

    def __getattribute__(self, name):
        """ When a schema field is called, we want to count that it
            was called.

        First, we need to figure out if it is a schema field and then
        we can count.

        If the schema field is not set, we throw a KeyError because
        your implementation apparently uses fields that do not exist.
        """

        if name == "_reserved_names":
            return object.__getattribute__(self, name)
        elif name in self._reserved_names():
            return object.__getattribute__(self, name)
        elif name in self._magic_attr_calls:
            self._magic_attr_calls[name] += 1

            return self._magic_attr_values[name]
        else:
            raise KeyError(
                f"{name} is not an attribute of the schema {self._schema}"
            )

    def __setattr__(self, name, value):
        """ When a schema field is called, we want to count that it
        was called. In this case we also need to assign the value.

        First, we need to figure out if it is a schema field and then
        we can count.

        If the schema field is not set, we throw a KeyError because
        your implementation apparently uses fields that do not exist."""

        if name == "_reserved_names":
            return object.__getattribute__(self, name)
        elif name in self._reserved_names():
            return object.__setattr__(self, name, value)
        elif name in self._magic_attr_calls:
            self._magic_attr_calls[name] += 1

            self._magic_attr_values[name] = MagicAttribute(value)
        else:
            raise KeyError(
                f"{name} is not an attribute of the schema {self._schema}"
            )

    def test_calls(self, key, number=1):
        """This tests whether or not the field has been called."""

        assert key in self._magic_attr_calls
        assert self._magic_attr_calls[key] >= number
