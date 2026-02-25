"""
Tests for database base configuration.

These tests verify that the declarative base is properly configured
and can be used for creating models.
"""

import pytest
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import DeclarativeMeta

from db.base import Base


def test_base_exists():
    """
    Test that the Base declarative class exists.
    
    This test verifies that we have a proper declarative base
    for SQLAlchemy models to inherit from.
    """
    assert Base is not None


def test_base_is_declarative():
    """
    Test that Base is a proper SQLAlchemy declarative base.
    
    This test verifies that Base has the metaclass required
    for declarative SQLAlchemy models.
    """
    assert isinstance(Base, DeclarativeMeta)


def test_base_can_create_model():
    """
    Test that we can create a model using the Base class.
    
    This test verifies that the Base class can be used to create
    SQLAlchemy models with proper table mapping.
    """
    # Create a simple test model
    class TestModel(Base):
        __tablename__ = "test_table"
        
        id = Column(Integer, primary_key=True)
        name = Column(String(50))
    
    # Verify the model was created correctly
    assert TestModel.__tablename__ == "test_table"
    assert hasattr(TestModel, "id")
    assert hasattr(TestModel, "name")
    assert hasattr(TestModel, "__table__")


def test_base_metadata():
    """
    Test that Base has metadata for managing schema.
    
    This test verifies that the Base class has metadata which
    is used by SQLAlchemy to track all tables and manage schema.
    """
    assert hasattr(Base, "metadata")
    assert Base.metadata is not None


def test_multiple_models_share_metadata():
    """
    Test that multiple models inherit the same metadata.
    
    This test verifies that all models created from Base share
    the same metadata instance, which is important for migrations
    and schema management.
    """
    class Model1(Base):
        __tablename__ = "model1_test"
        id = Column(Integer, primary_key=True)
    
    class Model2(Base):
        __tablename__ = "model2_test"
        id = Column(Integer, primary_key=True)
    
    # Both models should share the same metadata
    assert Model1.metadata is Model2.metadata
    assert Model1.metadata is Base.metadata
