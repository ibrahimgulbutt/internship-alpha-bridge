from sqlalchemy import (
    create_engine, Column, Integer, Float, String, Text, ForeignKey, DateTime, JSON, Index
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from sqlalchemy.dialects.postgresql import ARRAY
import datetime

Base = declarative_base()

class Product(Base):
    __tablename__ = "products"

    id = Column(Integer, primary_key=True)
    title = Column(String(500), nullable=False)
    description = Column(Text)
    category = Column(String(100), nullable=False, index=True)
    price = Column(Float, nullable=False)
    discount_percentage = Column(Float, default=0.0)
    rating = Column(Float)
    stock = Column(Integer, default=0)
    brand = Column(String(100), index=True)
    sku = Column(String(50), unique=True, index=True)
    weight = Column(Float)
    width = Column(Float)
    height = Column(Float)
    depth = Column(Float)
    warranty_information = Column(String(500))
    shipping_information = Column(String(500))
    availability_status = Column(String(50))
    return_policy = Column(String(500))
    minimum_order_quantity = Column(Integer, default=1)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    barcode = Column(String(50), unique=True)
    qr_code = Column(Text)
    thumbnail = Column(Text)

    # Relationships with cascade options
    tags = relationship("ProductTag", back_populates="product", cascade="all, delete-orphan")
    images = relationship("ProductImage", back_populates="product", cascade="all, delete-orphan")
    reviews = relationship("Review", back_populates="product", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Product(id={self.id}, title='{self.title}', price={self.price})>"

class ProductTag(Base):
    __tablename__ = "product_tags"

    id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey("products.id", ondelete="CASCADE"), nullable=False)
    tag = Column(String(100), nullable=False, index=True)

    product = relationship("Product", back_populates="tags")

    # Ensure unique tag per product
    __table_args__ = (Index('idx_product_tag_unique', 'product_id', 'tag', unique=True),)

    def __repr__(self):
        return f"<ProductTag(product_id={self.product_id}, tag='{self.tag}')>"

class ProductImage(Base):
    __tablename__ = "product_images"

    id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey("products.id", ondelete="CASCADE"), nullable=False)
    image_url = Column(Text, nullable=False)

    product = relationship("Product", back_populates="images")

    def __repr__(self):
        return f"<ProductImage(product_id={self.product_id}, image_url='{self.image_url[:50]}...')>"

class Review(Base):
    __tablename__ = "reviews"

    id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey("products.id", ondelete="CASCADE"), nullable=False)
    rating = Column(Integer, nullable=False)
    comment = Column(Text)
    date = Column(DateTime, nullable=False)
    reviewer_name = Column(String(200))
    reviewer_email = Column(String(300))

    product = relationship("Product", back_populates="reviews")

    def __repr__(self):
        return f"<Review(product_id={self.product_id}, rating={self.rating}, reviewer='{self.reviewer_name}')>"

# Database setup functions
def create_database_engine(database_url):
    
    engine = create_engine(database_url, echo=True)
    return engine

def create_tables(engine):
    
    Base.metadata.create_all(engine)
    print("All tables created successfully!")

def get_session(engine):
   
    Session = sessionmaker(bind=engine)
    return Session()

# Example usage:
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    
    # Load environment variables
    load_dotenv()
    
    # Get DATABASE_URL from environment variables
    DATABASE_URL = os.getenv('DATABASE_URL')
    
    if DATABASE_URL:
        print(f"Using database: {DATABASE_URL.split('@')[1] if '@' in DATABASE_URL else DATABASE_URL}")
        try:
            engine = create_database_engine(DATABASE_URL)
            create_tables(engine)
            print("Database tables created successfully!")
        except Exception as e:
            print(f"Error creating database tables: {e}")
    else:
        print("DATABASE_URL not found in environment variables!")
        print("Please check your .env file and ensure DATABASE_URL is set.")
        print("Example: DATABASE_URL=postgresql://postgres@localhost:5432/postgres")
