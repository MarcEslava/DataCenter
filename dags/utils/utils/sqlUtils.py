from sqlalchemy import Column, Integer, String, DECIMAL, Boolean, ForeignKey
from sqlalchemy.orm import declarative_base, relationship
# Declarative base class
Base = declarative_base()

# Define ORM models based on your database schema
class ProductsBrands(Base):
    __tablename__ = 'ProductsBrands'
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)

class PowerBILaboratories(Base):
    __tablename__ = 'PowerBILaboratories'
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)

class ProductsFamilies(Base):
    __tablename__ = 'ProductsFamilies'
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)

class ProductsSuperfamilies(Base):
    __tablename__ = 'ProductsSuperfamilies'
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)

class ProductsIvaTypes(Base):
    __tablename__ = 'ProductsIvaTypes'
    id = Column(Integer, primary_key=True)
    type = Column(String(50), nullable=False)

class Product(Base):
    __tablename__ = 'Products'
    id = Column(Integer, primary_key=True)
    CN6 = Column(String(50))
    EAN = Column(String(20), unique=True)
    description = Column(String(255))
    laboratory_id = Column(Integer, ForeignKey('PowerBILaboratories.id'))
    iva_type_id = Column(Integer, ForeignKey('ProductsIvaTypes.id'))
    PVL = Column(DECIMAL(10, 2))
    discount_book_1 = Column(DECIMAL(5, 2))
    discount_book_2 = Column(DECIMAL(5, 2))
    discount_book_3 = Column(DECIMAL(5, 2))
    minimum_units_book_1 = Column(Integer)
    minimum_units_book_2 = Column(Integer)
    minimum_units_book_3 = Column(Integer)
    family_id = Column(Integer, ForeignKey('ProductsFamilies.id'))
    active = Column(Boolean, default=True)
    
    laboratory = relationship("PowerBILaboratories")
    iva_type = relationship("ProductsIvaTypes")
    family = relationship("ProductsFamilies")