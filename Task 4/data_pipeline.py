import requests
import logging
from typing import List, Dict, Optional, Any
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
import time
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from models import (
    Product, ProductTag, ProductImage, Review, 
    create_database_engine, create_tables, get_session
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ProductDataPipeline:
    
    def __init__(self, database_url: str, base_api_url: str = "https://dummyjson.com"):

        self.database_url = database_url
        self.base_api_url = base_api_url
        self.session: Optional[Session] = None
        
        # Setup database connection
        try:
            self.engine = create_database_engine(database_url)
            logger.info("Database engine created successfully")
        except Exception as e:
            logger.error(f"Failed to create database engine: {e}")
            raise
    
    def _get_session(self) -> Session:
        
        if not self.session:
            self.session = get_session(self.engine)
        return self.session
    
    def _close_session(self):
        
        if self.session:
            self.session.close()
            self.session = None
    
    def extract_products_from_api(self, limit: int = 30, skip: int = 0) -> Dict[str, Any]:
        
        url = f"{self.base_api_url}/products"
        params = {
            'limit': limit,
            'skip': skip
        }
        
        try:
            logger.info(f"Fetching products from API: limit={limit}, skip={skip}")
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()  # Raises an HTTPError for bad responses
            
            data = response.json()
            logger.info(f"Successfully fetched {len(data['products'])} products")
            return data
            
        except requests.exceptions.Timeout:
            logger.error("API request timed out")
            raise
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error occurred: {e}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise
        except ValueError as e:
            logger.error(f"Failed to parse JSON response: {e}")
            raise
    
    def extract_all_products(self) -> List[Dict[str, Any]]:
        
        all_products = []
        limit = 30  # API default
        skip = 0
        
        logger.info("Starting to extract all products using pagination...")
        
        while True:
            try:
                # Get batch of products
                response_data = self.extract_products_from_api(limit=limit, skip=skip)
                products_batch = response_data['products']
                
                # Add to our collection
                all_products.extend(products_batch)
                
                # Check if we've got all products
                total_products = response_data['total']
                current_count = len(all_products)
                
                logger.info(f"Collected {current_count}/{total_products} products")
                
                # Break if we have all products
                if current_count >= total_products or len(products_batch) < limit:
                    break
                
                # Move to next page
                skip += limit
                
                # Be nice to the API - small delay between requests
                time.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Error during pagination at skip={skip}: {e}")
                break
        
        logger.info(f"Extraction complete. Total products collected: {len(all_products)}")
        return all_products
    
    def transform_product_data(self, product_data: Dict[str, Any]) -> Dict[str, Any]:
        
        # Extract dimensions
        dimensions = product_data.get('dimensions', {})
        
        # Extract meta information
        meta = product_data.get('meta', {})
        
        # Transform main product data
        transformed_product = {
            'id': product_data['id'],
            'title': product_data.get('title'),
            'description': product_data.get('description'),
            'category': product_data.get('category'),
            'price': product_data.get('price'),
            'discount_percentage': product_data.get('discountPercentage'),
            'rating': product_data.get('rating'),
            'stock': product_data.get('stock'),
            'brand': product_data.get('brand'),
            'sku': product_data.get('sku'),
            'weight': product_data.get('weight'),
            'width': dimensions.get('width'),
            'height': dimensions.get('height'),
            'depth': dimensions.get('depth'),
            'warranty_information': product_data.get('warrantyInformation'),
            'shipping_information': product_data.get('shippingInformation'),
            'availability_status': product_data.get('availabilityStatus'),
            'return_policy': product_data.get('returnPolicy'),
            'minimum_order_quantity': product_data.get('minimumOrderQuantity'),
            'created_at': self._parse_datetime(meta.get('createdAt')),
            'updated_at': self._parse_datetime(meta.get('updatedAt')),
            'barcode': meta.get('barcode'),
            'qr_code': meta.get('qrCode'),
            'thumbnail': product_data.get('thumbnail'),
        }
        
        # Transform related data
        transformed_data = {
            'product': transformed_product,
            'tags': product_data.get('tags', []),
            'images': product_data.get('images', []),
            'reviews': product_data.get('reviews', [])
        }
        
        return transformed_data
    
    def _parse_datetime(self, date_string: Optional[str]) -> Optional[datetime]:
        
        if not date_string:
            return None
        try:
            return datetime.fromisoformat(date_string.replace('Z', '+00:00'))
        except ValueError:
            logger.warning(f"Could not parse datetime: {date_string}")
            return None
    
    def load_product_to_database(self, transformed_data: Dict[str, Any]) -> bool:
        
        session = self._get_session()
        
        try:
            # Create Product object
            product_data = transformed_data['product']
            product = Product(**product_data)
            
            # Add product to session
            session.add(product)
            session.flush()  # This assigns the ID without committing
            
            # Add tags
            for tag_name in transformed_data['tags']:
                tag = ProductTag(product_id=product.id, tag=tag_name)
                session.add(tag)
            
            # Add images
            for image_url in transformed_data['images']:
                image = ProductImage(product_id=product.id, image_url=image_url)
                session.add(image)
            
            # Add reviews
            for review_data in transformed_data['reviews']:
                review = Review(
                    product_id=product.id,
                    rating=review_data.get('rating'),
                    comment=review_data.get('comment'),
                    date=self._parse_datetime(review_data.get('date')),
                    reviewer_name=review_data.get('reviewerName'),
                    reviewer_email=review_data.get('reviewerEmail')
                )
                session.add(review)
            
            # Commit all changes
            session.commit()
            logger.debug(f"Successfully loaded product {product.id}: {product.title}")
            return True
            
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Database error while loading product: {e}")
            return False
        except Exception as e:
            session.rollback()
            logger.error(f"Unexpected error while loading product: {e}")
            return False
    
    def run(self) -> Dict[str, int]:
        
        start_time = time.time()
        logger.info("Starting ProductDataPipeline.run()")
        
        # Initialize counters
        stats = {
            'total_products': 0,
            'successful_loads': 0,
            'failed_loads': 0,
            'execution_time_seconds': 0
        }
        
        try:
            # Step 1: Extract all products from API
            logger.info("Step 1: Extracting products from API...")
            all_products = self.extract_all_products()
            stats['total_products'] = len(all_products)
            
            if not all_products:
                logger.warning("No products found in API response")
                return stats
            
            # Step 2 & 3: Transform and Load each product
            logger.info("Step 2 & 3: Transforming and loading products to database...")
            
            for i, product_data in enumerate(all_products, 1):
                try:
                    # Transform data
                    transformed_data = self.transform_product_data(product_data)
                    
                    # Load to database
                    success = self.load_product_to_database(transformed_data)
                    
                    if success:
                        stats['successful_loads'] += 1
                    else:
                        stats['failed_loads'] += 1
                    
                    # Log progress every 50 products
                    if i % 50 == 0:
                        logger.info(f"Processed {i}/{stats['total_products']} products")
                        
                except Exception as e:
                    logger.error(f"Error processing product {i}: {e}")
                    stats['failed_loads'] += 1
                    continue
            
            # Calculate execution time
            stats['execution_time_seconds'] = round(time.time() - start_time, 2)
            
            # Log final results
            logger.info("Pipeline execution completed!")
            logger.info(f"Results: {stats}")
            
            return stats
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            raise
        finally:
            # Clean up database connection
            self._close_session()


def main():
    
    # Get DATABASE_URL from environment variables
    DATABASE_URL = os.getenv('DATABASE_URL')
    
    if not DATABASE_URL:
        print("Error: DATABASE_URL not found in environment variables!")
        print("Please check your .env file and ensure DATABASE_URL is set.")
        return
    
    print(f"Using database: {DATABASE_URL.split('@')[1] if '@' in DATABASE_URL else DATABASE_URL}")
    
    try:
        # Initialize pipeline
        pipeline = ProductDataPipeline(DATABASE_URL)
        
        # Create tables if they don't exist
        create_tables(pipeline.engine)
        
        # Run the pipeline
        results = pipeline.run()
        
        # Print results
        print("\n" + "="*50)
        print("PIPELINE EXECUTION SUMMARY")
        print("="*50)
        print(f"Total products processed: {results['total_products']}")
        print(f"Successful loads: {results['successful_loads']}")
        print(f"Failed loads: {results['failed_loads']}")
        print(f"Success rate: {(results['successful_loads']/results['total_products']*100):.1f}%")
        print(f"Execution time: {results['execution_time_seconds']} seconds")
        print("="*50)
        
    except Exception as e:
        logger.error(f"Main execution failed: {e}")
        print(f"Error: {e}")
        print("\nMake sure to:")
        print("1. Install required packages: pip install sqlalchemy psycopg2-binary requests python-dotenv")
        print("2. Update DATABASE_URL in your .env file")
        print("3. Ensure PostgreSQL is running and accessible")


if __name__ == "__main__":
    main()
