"""
SQLAlchemy ORM models for the wcfitting.db database.
This database contains EVE Online ship fittings, doctrines, and related SDE data.
"""

from sqlalchemy import String, Integer, DateTime, Float, Boolean, Text, ForeignKey, BigInteger
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from typing import Optional, List
from datetime import datetime


class FitBase(DeclarativeBase):
    """Base class for wcfitting database models."""
    pass


# ==================== Core Categories and Groups ====================

class FittingsCategory(FitBase):
    """Categories for organizing fittings and doctrines."""
    __tablename__ = "fittings_category"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    color: Mapped[str] = mapped_column(Text, nullable=False)
    
    # Relationships
    doctrines: Mapped[List["FittingsDoctrine"]] = relationship(
        secondary="fittings_category_doctrines",
        back_populates="categories"
    )
    fittings: Mapped[List["FittingsFitting"]] = relationship(
        secondary="fittings_category_fittings",
        back_populates="categories"
    )
    
    def __repr__(self) -> str:
        return f"FittingsCategory(id={self.id!r}, name={self.name!r}, color={self.color!r})"


class FittingsItemCategory(FitBase):
    """EVE item categories from SDE."""
    __tablename__ = "fittings_itemcategory"
    
    category_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(250), nullable=False)
    published: Mapped[int] = mapped_column(Integer, nullable=False)
    
    # Relationships
    groups: Mapped[List["FittingsItemGroup"]] = relationship(back_populates="category")
    
    def __repr__(self) -> str:
        return f"FittingsItemCategory(category_id={self.category_id!r}, name={self.name!r}, published={self.published!r})"


class FittingsItemGroup(FitBase):
    """EVE item groups from SDE."""
    __tablename__ = "fittings_itemgroup"
    
    group_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(250), nullable=False)
    published: Mapped[int] = mapped_column(Integer, nullable=False)
    category_id: Mapped[int] = mapped_column(Integer, ForeignKey("fittings_itemcategory.category_id"), nullable=False)
    
    # Relationships
    category: Mapped["FittingsItemCategory"] = relationship(back_populates="groups")
    types: Mapped[List["FittingsType"]] = relationship(back_populates="group")
    
    def __repr__(self) -> str:
        return f"FittingsItemGroup(group_id={self.group_id!r}, name={self.name!r}, category_id={self.category_id!r})"


# ==================== Type Information ====================

class FittingsType(FitBase):
    """EVE item types from SDE with detailed attributes."""
    __tablename__ = "fittings_type"
    
    type_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    type_name: Mapped[str] = mapped_column(String(500), nullable=False)
    published: Mapped[int] = mapped_column(Integer, nullable=False)
    mass: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    capacity: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    description: Mapped[Optional[str]] = mapped_column(String(5000), nullable=True)
    volume: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    packaged_volume: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    portion_size: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    radius: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    graphic_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    icon_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    market_group_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    group_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("fittings_itemgroup.group_id"), nullable=True)
    
    # Relationships
    group: Mapped[Optional["FittingsItemGroup"]] = relationship(back_populates="types")
    dogma_attributes: Mapped[List["FittingsDogmaAttribute"]] = relationship(back_populates="type")
    dogma_effects: Mapped[List["FittingsDogmaEffect"]] = relationship(back_populates="type")
    fittings: Mapped[List["FittingsFitting"]] = relationship(
        back_populates="ship_type",
        foreign_keys="[FittingsFitting.ship_type_id]"
    )
    fitting_items: Mapped[List["FittingsFittingItem"]] = relationship(back_populates="type_fk")
    
    def __repr__(self) -> str:
        return f"FittingsType(type_id={self.type_id!r}, type_name={self.type_name!r}, group_id={self.group_id!r})"


class FittingsTypeHistory(FitBase):
    """Historical record of type name changes."""
    __tablename__ = "fittings_typehistory"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    type_id: Mapped[int] = mapped_column(Integer, nullable=False)
    type_name: Mapped[str] = mapped_column(String(500), nullable=False)
    
    def __repr__(self) -> str:
        return f"FittingsTypeHistory(id={self.id!r}, type_id={self.type_id!r}, type_name={self.type_name!r})"


# ==================== Dogma Attributes and Effects ====================

class FittingsDogmaAttribute(FitBase):
    """Dogma attributes for types."""
    __tablename__ = "fittings_dogmaattribute"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    attribute_id: Mapped[int] = mapped_column(Integer, nullable=False)
    value: Mapped[float] = mapped_column(Float, nullable=False)
    type_id: Mapped[int] = mapped_column(Integer, ForeignKey("fittings_type.type_id"), nullable=False)
    
    # Relationships
    type: Mapped["FittingsType"] = relationship(back_populates="dogma_attributes")
    
    def __repr__(self) -> str:
        return f"FittingsDogmaAttribute(id={self.id!r}, attribute_id={self.attribute_id!r}, type_id={self.type_id!r}, value={self.value!r})"


class FittingsDogmaEffect(FitBase):
    """Dogma effects for types."""
    __tablename__ = "fittings_dogmaeffect"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    effect_id: Mapped[int] = mapped_column(Integer, nullable=False)
    is_default: Mapped[int] = mapped_column(Integer, nullable=False)
    type_id: Mapped[int] = mapped_column(Integer, ForeignKey("fittings_type.type_id"), nullable=False)
    
    # Relationships
    type: Mapped["FittingsType"] = relationship(back_populates="dogma_effects")
    
    def __repr__(self) -> str:
        return f"FittingsDogmaEffect(id={self.id!r}, effect_id={self.effect_id!r}, type_id={self.type_id!r}, is_default={self.is_default!r})"


# ==================== Doctrines ====================

class FittingsDoctrine(FitBase):
    """Doctrines (fleet compositions)."""
    __tablename__ = "fittings_doctrine"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    icon_url: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    created: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    last_updated: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    
    # Relationships
    categories: Mapped[List["FittingsCategory"]] = relationship(
        secondary="fittings_category_doctrines",
        back_populates="doctrines"
    )
    fittings: Mapped[List["FittingsFitting"]] = relationship(
        secondary="fittings_doctrine_fittings",
        back_populates="doctrines"
    )
    
    def __repr__(self) -> str:
        return f"FittingsDoctrine(id={self.id!r}, name={self.name!r}, created={self.created!r})"


class WatchDoctrines(FitBase):
    """Watched doctrines for monitoring."""
    __tablename__ = "watch_doctrines"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    icon_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    last_updated: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    def __repr__(self) -> str:
        return f"WatchDoctrines(id={self.id!r}, name={self.name!r})"


# ==================== Fittings ====================

class FittingsFitting(FitBase):
    """Ship fittings."""
    __tablename__ = "fittings_fitting"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    ship_type_type_id: Mapped[int] = mapped_column(Integer, nullable=False)
    ship_type_id: Mapped[int] = mapped_column(Integer, ForeignKey("fittings_type.type_id"), nullable=False)
    created: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_updated: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    
    # Relationships
    ship_type: Mapped["FittingsType"] = relationship(
        back_populates="fittings",
        foreign_keys=[ship_type_id]
    )
    items: Mapped[List["FittingsFittingItem"]] = relationship(back_populates="fit")
    categories: Mapped[List["FittingsCategory"]] = relationship(
        secondary="fittings_category_fittings",
        back_populates="fittings"
    )
    doctrines: Mapped[List["FittingsDoctrine"]] = relationship(
        secondary="fittings_doctrine_fittings",
        back_populates="fittings"
    )
    
    def __repr__(self) -> str:
        return f"FittingsFitting(id={self.id!r}, name={self.name!r}, ship_type_id={self.ship_type_id!r})"


class FittingsFittingItem(FitBase):
    """Items within a fitting."""
    __tablename__ = "fittings_fittingitem"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    flag: Mapped[str] = mapped_column(String(25), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    type_id: Mapped[int] = mapped_column(Integer, nullable=False)
    fit_id: Mapped[int] = mapped_column(Integer, ForeignKey("fittings_fitting.id"), nullable=False)
    type_fk_id: Mapped[int] = mapped_column(Integer, ForeignKey("fittings_type.type_id"), nullable=False)
    
    # Relationships
    fit: Mapped["FittingsFitting"] = relationship(back_populates="items")
    type_fk: Mapped["FittingsType"] = relationship(back_populates="fitting_items")
    
    def __repr__(self) -> str:
        return f"FittingsFittingItem(id={self.id!r}, fit_id={self.fit_id!r}, type_id={self.type_id!r}, quantity={self.quantity!r})"


# ==================== Junction Tables ====================

class FittingsCategoryDoctrines(FitBase):
    """Many-to-many relationship between categories and doctrines."""
    __tablename__ = "fittings_category_doctrines"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    category_id: Mapped[int] = mapped_column(Integer, ForeignKey("fittings_category.id"), nullable=False)
    doctrine_id: Mapped[int] = mapped_column(Integer, ForeignKey("fittings_doctrine.id"), nullable=False)
    
    def __repr__(self) -> str:
        return f"FittingsCategoryDoctrines(id={self.id!r}, category_id={self.category_id!r}, doctrine_id={self.doctrine_id!r})"


class FittingsCategoryFittings(FitBase):
    """Many-to-many relationship between categories and fittings."""
    __tablename__ = "fittings_category_fittings"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    category_id: Mapped[int] = mapped_column(Integer, ForeignKey("fittings_category.id"), nullable=False)
    fitting_id: Mapped[int] = mapped_column(Integer, ForeignKey("fittings_fitting.id"), nullable=False)
    
    def __repr__(self) -> str:
        return f"FittingsCategoryFittings(id={self.id!r}, category_id={self.category_id!r}, fitting_id={self.fitting_id!r})"


class FittingsCategoryGroups(FitBase):
    """Many-to-many relationship between categories and groups."""
    __tablename__ = "fittings_category_groups"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    category_id: Mapped[int] = mapped_column(Integer, ForeignKey("fittings_category.id"), nullable=False)
    group_id: Mapped[int] = mapped_column(Integer, nullable=False)
    
    def __repr__(self) -> str:
        return f"FittingsCategoryGroups(id={self.id!r}, category_id={self.category_id!r}, group_id={self.group_id!r})"


class FittingsDoctrineFittings(FitBase):
    """Many-to-many relationship between doctrines and fittings."""
    __tablename__ = "fittings_doctrine_fittings"
    
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    doctrine_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("fittings_doctrine.id"), nullable=False)
    fitting_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("fittings_fitting.id"), nullable=False)
    
    def __repr__(self) -> str:
        return f"FittingsDoctrineFittings(id={self.id!r}, doctrine_id={self.doctrine_id!r}, fitting_id={self.fitting_id!r})"


# ==================== Utility Tables ====================

class FittingsServerVersion(FitBase):
    """Server version tracking."""
    __tablename__ = "fittings_serverversion"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    
    def __repr__(self) -> str:
        return f"FittingsServerVersion(id={self.id!r})"


class JoinedInvTypes(FitBase):
    """Denormalized view of inventory types with joined metadata."""
    __tablename__ = "joinedinvtypes"
    
    typeID: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, primary_key=True)
    groupID: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    typeName: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    groupName: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    categoryID: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    categoryID_2: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    categoryName: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    metaGroupID: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    metaGroupID_2: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    metaGroupName: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    
    def __repr__(self) -> str:
        return f"JoinedInvTypes(typeID={self.typeID!r}, typeName={self.typeName!r}, groupName={self.groupName!r})"


if __name__ == "__main__":
    # Quick test to ensure models are properly defined
    from mkts_backend.config.config import DatabaseConfig
    
    # Get the wcfitting database config
    db_config = DatabaseConfig('wcfitting')
    
    # Create tables if they don't exist
    FitBase.metadata.create_all(db_config.engine)
    
    print("✓ All wcfitting models loaded successfully")
    print(f"✓ Tables: {', '.join(FitBase.metadata.tables.keys())}")
