from dataclasses import dataclass, field
from typing import Optional
from mkts_backend.config.config import DatabaseConfig
from sqlalchemy import text
from mkts_backend.config.logging_config import configure_logging

logger = configure_logging(__name__)


@dataclass
class TypeInfo:
    """
    A dataclass representing EVE Online type information from the SDE database.
    
    This class can be initialized with either a type_id or type_name, and will automatically
    fetch all other type information from the database.
    
    Args:
        type_id: The EVE type ID (e.g., 34 for Tritanium). Either type_id or type_name required.
        type_name: The EVE type name (e.g., "Tritanium"). Either type_id or type_name required.
        
    Attributes:
        type_id: The EVE type ID (populated automatically if initialized with type_name).
        type_name: The EVE type name (populated automatically if initialized with type_id).
        group_name: The group name of the type (e.g., "Mineral").
        category_name: The category name of the type (e.g., "Material").
        category_id: The category ID of the type.
        group_id: The group ID of the type.
        volume: The volume of the type in m³.
        
    Raises:
        ValueError: If neither type_id nor type_name is provided, or if both are provided.
        
    Examples:
        >>> # Initialize with type ID
        >>> trit = TypeInfo(type_id=34)
        >>> print(trit.type_name)
        'Tritanium'
        
        >>> # Initialize with type name
        >>> trit2 = TypeInfo(type_name="Tritanium")
        >>> print(trit2.type_id)
        34
        
        >>> # After initialization, both identifiers are available
        >>> trit3 = TypeInfo(type_id=34)
        >>> print(f"ID: {trit3.type_id}, Name: {trit3.type_name}")
        ID: 34, Name: Tritanium
    """
    type_id: Optional[int] = field(default=None)
    type_name: Optional[str] = field(default=None)
    group_name: str = field(init=False)
    category_name: str = field(init=False)
    category_id: int = field(init=False)
    group_id: int = field(init=False)
    volume: int = field(init=False)

    def __post_init__(self):
        """Validate input and populate all fields from the database."""
        # Validate that at least one identifier is provided
        if self.type_id is None and self.type_name is None:
            raise ValueError("Either type_id or type_name must be provided")
        
        if self.type_id is not None and self.type_name is not None:
            raise ValueError("Please provide either type_id or type_name, not both")
        
        self.get_type_info()

    def get_type_info(self):
        """
        Query the SDE database to populate all type information fields.
        
        Builds the appropriate SQL query based on whether type_id or type_name was provided,
        then populates all attributes including the missing identifier.
        """
        db = DatabaseConfig("sde")
        engine = db.engine
        
        # Build query based on which identifier was provided
        if self.type_id is not None:
            stmt = text("SELECT * FROM inv_info WHERE typeID = :identifier")
            params = {"identifier": self.type_id}
        else:
            stmt = text("SELECT * FROM inv_info WHERE typeName = :identifier")
            params = {"identifier": self.type_name}
        
        with engine.connect() as conn:
            result = conn.execute(stmt, params)
            for row in result:
                # Set both identifiers so they're both available after initialization
                self.type_id = row.typeID
                self.type_name = row.typeName
                self.group_name = row.groupName
                self.category_name = row.categoryName
                self.category_id = row.categoryID
                self.group_id = row.groupID
                self.volume = row.volume
        engine.dispose()


if __name__ == "__main__":
    # Example 1: Using type_id
    trit = TypeInfo(type_id=34)
    print(trit)
    
    # Example 2: Using type_name
    trit2 = TypeInfo(type_name="Tritanium")
    print(trit2)

