import logging
from typing import List, Dict, Optional, Any, Set
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import and_, or_
import time
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from models import (
    Product, ProductTag, ProductImage, Review, 
    create_database_engine, create_tables, get_session
)
from data_pipeline import ProductDataPipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ProductionDataPipeline(ProductDataPipeline):
    def __init__(self, database_url: str, base_api_url: str = "https://dummyjson.com"):

        super().__init__(database_url, base_api_url)
        self.current_run_timestamp = datetime.utcnow()
        logger.info("Production pipeline initialized")
    
    def get_existing_product_ids(self) -> Set[int]:
        
        session = self._get_session()
        try:
            existing_ids = session.query(Product.id).all()
            return {id_tuple[0] for id_tuple in existing_ids}
        except SQLAlchemyError as e:
            logger.error(f"Error fetching existing product IDs: {e}")
            return set()
    
    def upsert_product(self, transformed_data: Dict[str, Any]) -> bool:

        session = self._get_session()
        
        try:
            product_data = transformed_data['product']
            product_id = product_data['id']
            
            # Check if product exists
            existing_product = session.query(Product).filter(Product.id == product_id).first()
            
            if existing_product:
                # Update existing product
                logger.debug(f"Updating existing product {product_id}")
                for key, value in product_data.items():
                    if hasattr(existing_product, key):
                        setattr(existing_product, key, value)
                
                # Update the updated_at timestamp
                existing_product.updated_at = self.current_run_timestamp
                
                product = existing_product
            else:
                # Create new product
                logger.debug(f"Creating new product {product_id}")
                product = Product(**product_data)
                product.updated_at = self.current_run_timestamp
                session.add(product)
            
            # Handle related data with proper cleanup
            self._upsert_product_tags(session, product_id, transformed_data['tags'])
            self._upsert_product_images(session, product_id, transformed_data['images'])
            self._upsert_product_reviews(session, product_id, transformed_data['reviews'])
            
            session.commit()
            return True
            
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Database error during upsert for product {product_id}: {e}")
            return False
        except Exception as e:
            session.rollback()
            logger.error(f"Unexpected error during upsert for product {product_id}: {e}")
            return False
    
    def _upsert_product_tags(self, session: Session, product_id: int, new_tags: List[str]):

        # Remove existing tags for this product
        session.query(ProductTag).filter(ProductTag.product_id == product_id).delete()
        
        # Add new tags
        for tag_name in new_tags:
            tag = ProductTag(product_id=product_id, tag=tag_name)
            session.add(tag)
    
    def _upsert_product_images(self, session: Session, product_id: int, new_images: List[str]):
        
        # Remove existing images for this product
        session.query(ProductImage).filter(ProductImage.product_id == product_id).delete()
        
        # Add new images
        for image_url in new_images:
            image = ProductImage(product_id=product_id, image_url=image_url)
            session.add(image)
    
    def _upsert_product_reviews(self, session: Session, product_id: int, new_reviews: List[Dict]):
        
        # For simplicity in this exercise, we'll replace all reviews
        # In a real production system, you might want more sophisticated logic
        # to identify and update individual reviews
        
        session.query(Review).filter(Review.product_id == product_id).delete()
        
        # Add new reviews
        for review_data in new_reviews:
            review = Review(
                product_id=product_id,
                rating=review_data.get('rating'),
                comment=review_data.get('comment'),
                date=self._parse_datetime(review_data.get('date')),
                reviewer_name=review_data.get('reviewerName'),
                reviewer_email=review_data.get('reviewerEmail')
            )
            session.add(review)
    
    def remove_stale_products(self, current_api_product_ids: Set[int]) -> int:
        
        session = self._get_session()
        
        try:
            # Find products in database that are not in current API response
            existing_ids = self.get_existing_product_ids()
            stale_ids = existing_ids - current_api_product_ids
            
            if not stale_ids:
                logger.info("No stale products found")
                return 0
            
            logger.info(f"Found {len(stale_ids)} stale products to remove")
            
            # Remove stale products (cascade will handle related records)
            removed_count = session.query(Product).filter(Product.id.in_(stale_ids)).delete(synchronize_session=False)
            session.commit()
            
            logger.info(f"Removed {removed_count} stale products")
            return removed_count
            
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error removing stale products: {e}")
            return 0
    
    def run(self) -> Dict[str, int]:
        
        start_time = time.time()
        logger.info("Starting Production Pipeline run()")
        
        # Initialize detailed counters
        stats = {
            'total_api_products': 0,
            'new_products': 0,
            'updated_products': 0,
            'failed_upserts': 0,
            'stale_products_removed': 0,
            'execution_time_seconds': 0,
            'database_products_before': 0,
            'database_products_after': 0
        }
        
        try:
            # Get initial database state
            stats['database_products_before'] = len(self.get_existing_product_ids())
            logger.info(f"Database contains {stats['database_products_before']} products before pipeline run")
            
            # Step 1: Extract all products from API
            logger.info("Step 1: Extracting products from API...")
            all_products = self.extract_all_products()
            stats['total_api_products'] = len(all_products)
            
            if not all_products:
                logger.warning("No products found in API response")
                return stats
            
            # Collect API product IDs for stale product detection
            api_product_ids = {product['id'] for product in all_products}
            
            # Step 2: Process each product with upsert logic
            logger.info("Step 2: Processing products with upsert logic...")
            
            existing_ids_before = self.get_existing_product_ids()
            
            for i, product_data in enumerate(all_products, 1):
                try:
                    # Transform data
                    transformed_data = self.transform_product_data(product_data)
                    product_id = transformed_data['product']['id']
                    
                    # Determine if this is new or update
                    is_new_product = product_id not in existing_ids_before
                    
                    # Upsert to database
                    success = self.upsert_product(transformed_data)
                    
                    if success:
                        if is_new_product:
                            stats['new_products'] += 1
                        else:
                            stats['updated_products'] += 1
                    else:
                        stats['failed_upserts'] += 1
                    
                    # Log progress every 50 products
                    if i % 50 == 0:
                        logger.info(f"Processed {i}/{stats['total_api_products']} products")
                        
                except Exception as e:
                    logger.error(f"Error processing product {i}: {e}")
                    stats['failed_upserts'] += 1
                    continue
            
            # Step 3: Remove stale products
            logger.info("Step 3: Removing stale products...")
            stats['stale_products_removed'] = self.remove_stale_products(api_product_ids)
            
            # Get final database state
            stats['database_products_after'] = len(self.get_existing_product_ids())
            
            # Calculate execution time
            stats['execution_time_seconds'] = round(time.time() - start_time, 2)
            
            # Log comprehensive results
            logger.info("Production pipeline execution completed!")
            logger.info("="*60)
            logger.info("PRODUCTION PIPELINE RESULTS:")
            logger.info(f"  API Products Found: {stats['total_api_products']}")
            logger.info(f"  New Products Added: {stats['new_products']}")
            logger.info(f"  Existing Products Updated: {stats['updated_products']}")
            logger.info(f"  Failed Operations: {stats['failed_upserts']}")
            logger.info(f"  Stale Products Removed: {stats['stale_products_removed']}")
            logger.info(f"  Database Before: {stats['database_products_before']} products")
            logger.info(f"  Database After: {stats['database_products_after']} products")
            logger.info(f"  Execution Time: {stats['execution_time_seconds']} seconds")
            logger.info("="*60)
            
            return stats
            
        except Exception as e:
            logger.error(f"Production pipeline execution failed: {e}")
            raise
        finally:
            # Clean up database connection
            self._close_session()
    
    def validate_data_integrity(self) -> Dict[str, Any]:
        
        session = self._get_session()
        
        try:
            validation_results = {}
            
            # Count records in each table
            validation_results['product_count'] = session.query(Product).count()
            validation_results['tag_count'] = session.query(ProductTag).count()
            validation_results['image_count'] = session.query(ProductImage).count()
            validation_results['review_count'] = session.query(Review).count()
            
            # Check for orphaned records (shouldn't exist due to foreign keys)
            orphaned_tags = session.query(ProductTag).filter(
                ~ProductTag.product_id.in_(session.query(Product.id))
            ).count()
            
            orphaned_images = session.query(ProductImage).filter(
                ~ProductImage.product_id.in_(session.query(Product.id))
            ).count()
            
            orphaned_reviews = session.query(Review).filter(
                ~Review.product_id.in_(session.query(Product.id))
            ).count()
            
            validation_results['orphaned_tags'] = orphaned_tags
            validation_results['orphaned_images'] = orphaned_images
            validation_results['orphaned_reviews'] = orphaned_reviews
            
            # Check for duplicate products by ID
            duplicate_products = session.query(Product.id).group_by(Product.id).having(
                session.query(Product.id).filter(Product.id == Product.id).count() > 1
            ).count()
            
            validation_results['duplicate_products'] = duplicate_products
            
            logger.info("Data integrity validation completed")
            logger.info(f"Validation results: {validation_results}")
            
            return validation_results
            
        except Exception as e:
            logger.error(f"Data integrity validation failed: {e}")
            return {'error': str(e)}


def main():
    
    # Get DATABASE_URL from environment variables
    DATABASE_URL = os.getenv('DATABASE_URL')
    
    if not DATABASE_URL:
        print("Error: DATABASE_URL not found in environment variables!")
        print("Please check your .env file and ensure DATABASE_URL is set.")
        return
    
    print(f"Using database: {DATABASE_URL.split('@')[1] if '@' in DATABASE_URL else DATABASE_URL}")
    
    try:
        # Initialize production pipeline
        pipeline = ProductionDataPipeline(DATABASE_URL)
        
        # Create tables if they don't exist
        create_tables(pipeline.engine)
        
        # Run the pipeline multiple times to demonstrate no duplicates
        print("\n" + "="*60)
        print("DEMONSTRATING PRODUCTION PIPELINE")
        print("="*60)
        
        for run_number in range(1, 3):  # Run twice to show no duplicates
            print(f"\n--- PIPELINE RUN #{run_number} ---")
            
            results = pipeline.run()
            
            # Validate data integrity
            validation = pipeline.validate_data_integrity()
            
            # Print summary
            print(f"\nRun {run_number} Summary:")
            print(f"  Database products after run: {results['database_products_after']}")
            print(f"  New products: {results['new_products']}")
            print(f"  Updated products: {results['updated_products']}")
            print(f"  Orphaned records: {validation['orphaned_tags'] + validation['orphaned_images'] + validation['orphaned_reviews']}")
            print(f"  Duplicate products: {validation['duplicate_products']}")
            
            if run_number < 2:
                print("\nWaiting 2 seconds before next run...")
                time.sleep(2)
        
        print("\n" + "="*60)
        print("PRODUCTION PIPELINE DEMONSTRATION COMPLETE")
        print("Notice: Second run shows 0 new products and all updates,")
        print("proving no duplicates are created!")
        print("="*60)
        
    except Exception as e:
        logger.error(f"Main execution failed: {e}")
        print(f"Error: {e}")
        print("\nMake sure to:")
        print("1. Install required packages: pip install sqlalchemy psycopg2-binary requests python-dotenv")
        print("2. Update DATABASE_URL in your .env file")
        print("3. Ensure PostgreSQL is running and accessible")


if __name__ == "__main__":
    main()
