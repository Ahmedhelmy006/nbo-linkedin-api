"""
Subscriber repository for database operations.
"""
import logging
from typing import Optional, Dict, Any
from database.connection import db_manager

logger = logging.getLogger(__name__)

class SubscriberRepository:
    """
    Repository for subscriber database operations.
    """
    
    async def get_subscriber_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        """
        Get subscriber record by email address.
        
        Args:
            email: Email address to search for
            
        Returns:
            Dict with subscriber data or None if not found
        """
        try:
            email = email.lower().strip()
            query = "SELECT id, email_address, linkedin_profile_url FROM subscribers WHERE email_address = $1"
            
            result = await db_manager.execute_single(query, email)
            
            if result:
                logger.info(f"Subscriber found for email: {email}")
                return {
                    'id': result['id'],
                    'email_address': result['email_address'],
                    'linkedin_url': result['linkedin_profile_url']  # Map to our internal field name
                }
            else:
                logger.info(f"No subscriber found for email: {email}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting subscriber by email {email}: {e}")
            raise
    
    async def get_linkedin_url(self, email: str) -> Optional[str]:
        """
        Get LinkedIn URL for a specific email.
        
        Args:
            email: Email address
            
        Returns:
            LinkedIn URL if exists, None otherwise
        """
        try:
            subscriber = await self.get_subscriber_by_email(email)
            
            if subscriber and subscriber.get('linkedin_url'):
                linkedin_url = subscriber['linkedin_url']
                logger.info(f"LinkedIn URL found for {email}: {linkedin_url}")
                return linkedin_url
            else:
                logger.info(f"No LinkedIn URL found for {email}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting LinkedIn URL for {email}: {e}")
            return None
    
    async def update_linkedin_url(self, email: str, linkedin_url: str) -> bool:
        """
        Update LinkedIn URL for a subscriber.
        
        Args:
            email: Email address
            linkedin_url: LinkedIn URL to save
            
        Returns:
            True if successful, False otherwise
        """
        try:
            email = email.lower().strip()
            query = "UPDATE subscribers SET linkedin_profile_url = $1 WHERE email_address = $2"
            
            success = await db_manager.execute_update(query, linkedin_url, email)
            
            if success:
                logger.info(f"LinkedIn URL updated for {email}: {linkedin_url}")
            else:
                logger.error(f"Failed to update LinkedIn URL for {email}")
            
            return success
            
        except Exception as e:
            logger.error(f"Error updating LinkedIn URL for {email}: {e}")
            return False
    
    async def subscriber_exists(self, email: str) -> bool:
        """
        Check if a subscriber exists in the database.
        
        Args:
            email: Email address to check
            
        Returns:
            True if subscriber exists, False otherwise
        """
        try:
            subscriber = await self.get_subscriber_by_email(email)
            exists = subscriber is not None
            logger.info(f"Subscriber exists check for {email}: {exists}")
            return exists
            
        except Exception as e:
            logger.error(f"Error checking if subscriber exists for {email}: {e}")
            return False
    
    async def get_subscriber_stats(self) -> Dict[str, int]:
        """
        Get basic statistics about subscribers.
        
        Returns:
            Dict with subscriber statistics
        """
        try:
            # Total subscribers
            total_query = "SELECT COUNT(*) as total FROM subscribers"
            total_result = await db_manager.execute_single(total_query)
            total_subscribers = total_result['total'] if total_result else 0
            
            # Subscribers with LinkedIn URLs
            linkedin_query = "SELECT COUNT(*) as with_linkedin FROM subscribers WHERE linkedin_profile_url IS NOT NULL"
            linkedin_result = await db_manager.execute_single(linkedin_query)
            with_linkedin = linkedin_result['with_linkedin'] if linkedin_result else 0
            
            stats = {
                'total_subscribers': total_subscribers,
                'with_linkedin_url': with_linkedin,
                'without_linkedin_url': total_subscribers - with_linkedin
            }
            
            logger.info(f"Subscriber stats: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Error getting subscriber stats: {e}")
            return {
                'total_subscribers': 0,
                'with_linkedin_url': 0,
                'without_linkedin_url': 0
            }

# Global repository instance
subscriber_repo = SubscriberRepository()
