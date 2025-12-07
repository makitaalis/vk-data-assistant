"""
Database connection module that extends the base VKDatabase class
with additional functionality for the bot application
"""

import sys
from pathlib import Path

# Add the project root directory to Python path to enable importing root modules
root_dir = Path(__file__).parent.parent
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

# Import the VKDatabase class from the root database.py file
import database  # This imports the root database.py file, not the package

# Create an extended version with additional methods
class ExtendedVKDatabase(database.VKDatabase):
    """Extended database class with additional user and statistics methods"""

    async def get_all_users(self):
        """Retrieve all users from the database"""
        async with self.acquire() as conn:
            rows = await conn.fetch("""
                SELECT 
                    user_id,
                    username,
                    first_name,
                    last_name,
                    accepted_disclaimer,
                    last_activity
                FROM users
                ORDER BY last_activity DESC
            """)

            return [dict(row) for row in rows]

    async def get_users_statistics(self):
        """Calculate aggregate statistics for all users"""
        async with self.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_users,
                    COUNT(*) FILTER (WHERE last_activity > NOW() - INTERVAL '7 days') as active_7d,
                    COUNT(*) FILTER (WHERE last_activity > NOW() - INTERVAL '30 days') as active_30d
                FROM users
            """)

            return {
                "total_users": row["total_users"] or 0,
                "active_7d": row["active_7d"] or 0,
                "active_30d": row["active_30d"] or 0
            }

    async def get_user_statistics_by_period(self, user_id: int, days: int):
        """Retrieve user statistics for a specific time period"""
        async with self.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as checked,
                    COUNT(*) FILTER (WHERE found_data = TRUE) as found
                FROM vk_results
                WHERE checked_by_user_id = $1
                AND checked_at > NOW() - INTERVAL '%s days'
            """ % days, user_id)

            return {
                "checked": row["checked"] or 0,
                "found": row["found"] or 0
            }

    async def get_top_users(self, limit: int = 10):
        """Retrieve the most active users based on total checks"""
        async with self.acquire() as conn:
            rows = await conn.fetch("""
                SELECT 
                    u.user_id,
                    u.username,
                    u.first_name,
                    u.last_name,
                    COUNT(r.link) as total_checked,
                    COUNT(r.link) FILTER (WHERE r.found_data = TRUE) as found_data
                FROM users u
                LEFT JOIN vk_results r ON u.user_id = r.checked_by_user_id
                GROUP BY u.user_id, u.username, u.first_name, u.last_name
                HAVING COUNT(r.link) > 0
                ORDER BY total_checked DESC
                LIMIT $1
            """, limit)

            return [dict(row) for row in rows]


# Export the extended class as VKDatabase for backward compatibility
VKDatabase = ExtendedVKDatabase

__all__ = ["VKDatabase"]