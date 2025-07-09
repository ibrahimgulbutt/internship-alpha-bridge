"""
Exercise 3-1: Code Review - Product Update Implementation
========================================================

COMPREHENSIVE CODE REVIEW OF JUNIOR DEVELOPER'S PRODUCT UPDATER

Date: July 8, 2025
Reviewer: Senior Data Engineer
Code Under Review: ProductUpdater class for updating products via API
"""

# =============================================================================
# CODE QUALITY ISSUES
# =============================================================================

"""
1. IMPROPER ERROR HANDLING
   
   Problem: The code catches generic Exception without proper logging or recovery
   Why it's problematic: Makes debugging difficult and hides important error information
   How to fix: Use specific exception types and proper logging
   
   Current code:
   ```python
   except Exception as e:
       print(f"Error on attempt {attempt + 1}: {str(e)}")
   ```
   
   Should be:
   ```python
   except requests.exceptions.RequestException as e:
       logger.error(f"Network error on attempt {attempt + 1}: {str(e)}")
   except requests.exceptions.Timeout as e:
       logger.warning(f"Timeout on attempt {attempt + 1}: {str(e)}")
   except Exception as e:
       logger.critical(f"Unexpected error on attempt {attempt + 1}: {str(e)}")
   ```
"""

"""
2. INCONSISTENT RETURN TYPES AND ERROR SIGNALING
   
   Problem: Methods return None on failure but this isn't consistently checked
   Why it's problematic: Calling code can't reliably detect failures
   How to fix: Use exceptions for errors or consistent return patterns
   
   Current code returns None on failure:
   ```python
   if response.status_code == 200:
       return response.json()
   else:
       return None
   ```
   
   Better approach:
   ```python
   response.raise_for_status()  # Raises exception for HTTP errors
   return response.json()
   ```
"""

"""
3. MISSING INPUT VALIDATION
   
   Problem: No validation of input parameters
   Why it's problematic: Can lead to API errors or security issues
   How to fix: Add parameter validation
   
   Missing validations:
   - product_id should be positive integer
   - price should be positive number
   - title should not be empty
   - description length limits
"""

"""
4. NO LOGGING FRAMEWORK
   
   Problem: Uses print() statements instead of proper logging
   Why it's problematic: No log levels, no structured logging, hard to debug production issues
   How to fix: Use Python's logging module
"""

"""
5. HARDCODED VALUES AND NO CONFIGURATION
   
   Problem: Base URL, timeouts, retry counts are hardcoded
   Why it's problematic: Hard to configure for different environments
   How to fix: Use configuration files or environment variables
"""

# =============================================================================
# SECURITY CONCERNS
# =============================================================================

"""
1. NO INPUT SANITIZATION
   
   Problem: User input is directly passed to API without sanitization
   Why it's problematic: Potential for injection attacks or malformed requests
   Security Risk: Medium
   How to fix: Validate and sanitize all inputs
"""

"""
2. NO AUTHENTICATION OR AUTHORIZATION
   
   Problem: No API key, token, or authentication mechanism
   Why it's problematic: Anyone can make requests, no rate limiting protection
   Security Risk: High in production
   How to fix: Implement proper authentication
"""

"""
3. POTENTIAL DATA EXPOSURE
   
   Problem: Error messages might expose sensitive information
   Why it's problematic: Could leak API structure or internal details
   Security Risk: Low to Medium
   How to fix: Sanitize error messages for external consumption
"""

# =============================================================================
# BEST PRACTICES VIOLATIONS
# =============================================================================

"""
1. NO TYPE HINTS
   
   Problem: No type annotations for parameters and return values
   Why it's problematic: Reduces code readability and IDE support
   Best Practice: Use typing module for all function signatures
"""

"""
2. POOR CLASS DESIGN
   
   Problem: Class has no state but could benefit from configuration
   Why it's problematic: Not following OOP principles properly
   Best Practice: Make class configurable and stateful where appropriate
"""

"""
3. NO DOCUMENTATION
   
   Problem: Missing docstrings and comments
   Why it's problematic: Code is hard to understand and maintain
   Best Practice: Add comprehensive docstrings and inline comments
"""

"""
4. MIXING CONCERNS
   
   Problem: Single class handles HTTP requests, retries, and business logic
   Why it's problematic: Violates Single Responsibility Principle
   Best Practice: Separate HTTP client, retry logic, and business operations
"""

# =============================================================================
# PERFORMANCE PROBLEMS
# =============================================================================

"""
1. INEFFICIENT BULK OPERATIONS
   
   Problem: Makes individual API calls in a loop with sleep delays
   Why it's problematic: Very slow for large datasets
   Performance Impact: High
   How to fix: Use batch API endpoints if available, or implement proper concurrency
"""

"""
2. NO CONNECTION POOLING
   
   Problem: Creates new HTTP connections for each request
   Why it's problematic: Unnecessary overhead and slower performance
   Performance Impact: Medium
   How to fix: Use requests.Session() for connection reuse
"""

"""
3. BLOCKING OPERATIONS
   
   Problem: Synchronous operations block the thread
   Why it's problematic: Poor performance for concurrent operations
   Performance Impact: High for bulk operations
   How to fix: Use async/await or threading for concurrent requests
"""

"""
4. NO CACHING
   
   Problem: No caching of responses or connection reuse
   Why it's problematic: Unnecessary network calls
   Performance Impact: Medium
   How to fix: Implement appropriate caching strategies
"""

# =============================================================================
# IMPROVED IMPLEMENTATION
# =============================================================================

import logging
import requests
import time
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class UpdateRequest:
    """Data class for product update requests with validation."""
    product_id: int
    title: str
    price: float
    description: Optional[str] = None
    
    def __post_init__(self):
        """Validate inputs after initialization."""
        if self.product_id <= 0:
            raise ValueError("Product ID must be positive")
        if self.price <= 0:
            raise ValueError("Price must be positive")
        if not self.title or not self.title.strip():
            raise ValueError("Title cannot be empty")
        if self.description and len(self.description) > 1000:
            raise ValueError("Description too long (max 1000 characters)")

@dataclass
class ProductUpdateConfig:
    """Configuration for the product updater."""
    base_url: str = "https://dummyjson.com"
    timeout: int = 30
    max_retries: int = 3
    retry_delay: float = 1.0
    rate_limit_delay: float = 0.1
    max_concurrent_requests: int = 10

class ProductUpdateResult:
    """Result object for update operations."""
    def __init__(self, success: bool, data: Optional[Dict] = None, error: Optional[str] = None):
        self.success = success
        self.data = data
        self.error = error
        self.timestamp = time.time()

class HTTPClient:
    """Dedicated HTTP client with proper session management and retries."""
    
    def __init__(self, config: ProductUpdateConfig):
        self.config = config
        self.session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=config.max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Set default headers
        self.session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': 'ProductUpdater/1.0'
        })
    
    def put(self, url: str, json_data: Dict) -> requests.Response:
        """Make PUT request with proper error handling."""
        try:
            response = self.session.put(
                url, 
                json=json_data,  # Use json parameter instead of manual serialization
                timeout=self.config.timeout
            )
            response.raise_for_status()
            return response
        except requests.exceptions.Timeout:
            logger.error(f"Request timeout for URL: {url}")
            raise
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error {e.response.status_code} for URL: {url}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for URL: {url}: {str(e)}")
            raise
    
    def close(self):
        """Close the session."""
        self.session.close()

class ImprovedProductUpdater:
    """
    Production-ready product updater with proper error handling, logging,
    performance optimizations, and security considerations.
    """
    
    def __init__(self, config: Optional[ProductUpdateConfig] = None):
        """Initialize with configuration."""
        self.config = config or ProductUpdateConfig()
        self.http_client = HTTPClient(self.config)
        logger.info("ProductUpdater initialized")
    
    def update_product(self, update_request: UpdateRequest) -> ProductUpdateResult:
        """
        Update a single product with comprehensive error handling.
        
        Args:
            update_request: Validated update request object
            
        Returns:
            ProductUpdateResult: Result of the update operation
        """
        url = f"{self.config.base_url}/products/{update_request.product_id}"
        
        # Prepare payload
        payload = {
            "title": update_request.title.strip(),
            "price": update_request.price,
        }
        
        if update_request.description:
            payload["description"] = update_request.description.strip()
        
        try:
            logger.info(f"Updating product {update_request.product_id}")
            response = self.http_client.put(url, payload)
            
            result_data = response.json()
            logger.info(f"Successfully updated product {update_request.product_id}")
            
            return ProductUpdateResult(
                success=True,
                data=result_data
            )
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Failed to update product {update_request.product_id}: {str(e)}"
            logger.error(error_msg)
            
            return ProductUpdateResult(
                success=False,
                error=error_msg
            )
        except Exception as e:
            error_msg = f"Unexpected error updating product {update_request.product_id}: {str(e)}"
            logger.critical(error_msg)
            
            return ProductUpdateResult(
                success=False,
                error=error_msg
            )
    
    def bulk_update_products(self, update_requests: List[UpdateRequest]) -> List[ProductUpdateResult]:
        """
        Update multiple products with improved performance and error handling.
        
        Args:
            update_requests: List of validated update requests
            
        Returns:
            List of ProductUpdateResult objects
        """
        if not update_requests:
            logger.warning("No update requests provided")
            return []
        
        logger.info(f"Starting bulk update of {len(update_requests)} products")
        results = []
        
        # Use ThreadPoolExecutor for concurrent requests (respecting rate limits)
        with ThreadPoolExecutor(max_workers=self.config.max_concurrent_requests) as executor:
            # Submit all tasks
            future_to_request = {
                executor.submit(self.update_product, request): request 
                for request in update_requests
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_request):
                request = future_to_request[future]
                try:
                    result = future.result()
                    results.append(result)
                    
                    # Rate limiting
                    time.sleep(self.config.rate_limit_delay)
                    
                except Exception as e:
                    error_msg = f"Task failed for product {request.product_id}: {str(e)}"
                    logger.error(error_msg)
                    results.append(ProductUpdateResult(
                        success=False,
                        error=error_msg
                    ))
        
        # Log summary
        successful_updates = sum(1 for r in results if r.success)
        logger.info(f"Bulk update completed: {successful_updates}/{len(results)} successful")
        
        return results
    
    async def async_bulk_update_products(self, update_requests: List[UpdateRequest]) -> List[ProductUpdateResult]:
        """
        Asynchronous bulk update for even better performance.
        
        Args:
            update_requests: List of validated update requests
            
        Returns:
            List of ProductUpdateResult objects
        """
        if not update_requests:
            return []
        
        logger.info(f"Starting async bulk update of {len(update_requests)} products")
        
        async with aiohttp.ClientSession() as session:
            tasks = []
            for request in update_requests:
                task = self._async_update_single_product(session, request)
                tasks.append(task)
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Convert exceptions to error results
            processed_results = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    processed_results.append(ProductUpdateResult(
                        success=False,
                        error=f"Async error for product {update_requests[i].product_id}: {str(result)}"
                    ))
                else:
                    processed_results.append(result)
            
            successful_updates = sum(1 for r in processed_results if r.success)
            logger.info(f"Async bulk update completed: {successful_updates}/{len(processed_results)} successful")
            
            return processed_results
    
    async def _async_update_single_product(self, session: aiohttp.ClientSession, request: UpdateRequest) -> ProductUpdateResult:
        """Helper method for async single product update."""
        url = f"{self.config.base_url}/products/{request.product_id}"
        
        payload = {
            "title": request.title.strip(),
            "price": request.price,
        }
        
        if request.description:
            payload["description"] = request.description.strip()
        
        try:
            async with session.put(url, json=payload, timeout=self.config.timeout) as response:
                if response.status == 200:
                    data = await response.json()
                    return ProductUpdateResult(success=True, data=data)
                else:
                    error_msg = f"HTTP {response.status} for product {request.product_id}"
                    return ProductUpdateResult(success=False, error=error_msg)
                    
        except Exception as e:
            error_msg = f"Async error for product {request.product_id}: {str(e)}"
            return ProductUpdateResult(success=False, error=error_msg)
    
    def get_update_statistics(self, results: List[ProductUpdateResult]) -> Dict[str, Any]:
        """Generate comprehensive statistics from update results."""
        if not results:
            return {"total": 0, "successful": 0, "failed": 0, "success_rate": 0.0}
        
        successful = sum(1 for r in results if r.success)
        failed = len(results) - successful
        success_rate = (successful / len(results)) * 100
        
        # Calculate timing statistics
        timestamps = [r.timestamp for r in results]
        duration = max(timestamps) - min(timestamps) if timestamps else 0
        
        return {
            "total": len(results),
            "successful": successful,
            "failed": failed,
            "success_rate": round(success_rate, 2),
            "duration_seconds": round(duration, 2),
            "requests_per_second": round(len(results) / duration, 2) if duration > 0 else 0
        }
    
    def close(self):
        """Clean up resources."""
        self.http_client.close()
        logger.info("ProductUpdater closed")

# =============================================================================
# USAGE EXAMPLES
# =============================================================================

def demonstrate_improved_implementation():
    """Demonstrate the improved implementation."""
    
    # Create configuration
    config = ProductUpdateConfig(
        max_retries=3,
        timeout=30,
        rate_limit_delay=0.1,
        max_concurrent_requests=5
    )
    
    # Create updater
    updater = ImprovedProductUpdater(config)
    
    try:
        # Single update with validation
        update_request = UpdateRequest(
            product_id=1,
            title="iPhone 15 Pro Max",
            price=1299.99,
            description="Latest iPhone with advanced features"
        )
        
        result = updater.update_product(update_request)
        
        if result.success:
            print(f"Update successful: {result.data}")
        else:
            print(f"Update failed: {result.error}")
        
        # Bulk updates
        bulk_requests = [
            UpdateRequest(1, "iPhone 15 Pro", 1499.99),
            UpdateRequest(2, "Samsung Galaxy S24", 1199.99),
            UpdateRequest(3, "Google Pixel 8", 799.99, "Latest Google phone"),
        ]
        
        bulk_results = updater.bulk_update_products(bulk_requests)
        stats = updater.get_update_statistics(bulk_results)
        
        print(f"Bulk update statistics: {stats}")
        
    finally:
        updater.close()

if __name__ == "__main__":
    demonstrate_improved_implementation()
