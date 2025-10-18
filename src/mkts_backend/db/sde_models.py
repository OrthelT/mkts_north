from sqlalchemy import String, Integer, DateTime, Float, Boolean, event
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from mkts_backend.utils.utils import get_type_name
from mkts_backend.config.config import DatabaseConfig

class Base(DeclarativeBase):
    pass

class SdeInfo(Base):
    __tablename__ = "inv_info"
    
    typeID: Mapped[int] = mapped_column(Integer, primary_key=True)
    typeName: Mapped[str] = mapped_column(String)
    groupID: Mapped[int] = mapped_column(Integer)
    volume: Mapped[float] = mapped_column(Float)
    groupName: Mapped[str] = mapped_column(String)
    categoryID: Mapped[int] = mapped_column(Integer)
    categoryName: Mapped[str] = mapped_column(String)
    
    def __repr__(self) -> str:
        return f"<SdeInfo(typeID={self.typeID}, typeName='{self.typeName}', groupName='{self.groupName}', categoryName='{self.categoryName}')>"
