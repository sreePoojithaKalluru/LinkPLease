"""
app/routes/rules.py
────────────────────
POST /rules — create a new keyword → DM mapping.

Response contract (non-negotiable):
  - 201 with {"rule_id": str, "keyword": str, "dm_message": str}
"""
import logging

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import Rule
from app.schemas import RuleCreate, RuleResponse

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/rules", response_model=RuleResponse, status_code=201)
async def create_rule(body: RuleCreate, db: AsyncSession = Depends(get_db)) -> RuleResponse:
    """
    Create a new keyword rule.

    The keyword is stored in its original case; matching at query time is done
    case-insensitively (LOWER(text) LIKE '%keyword%' or Python .lower()).
    """
    rule = Rule(
        keyword=body.keyword,
        dm_message=body.dm_message,
    )
    db.add(rule)
    await db.commit()
    await db.refresh(rule)

    logger.info("rules: created rule_id=%s keyword=%r", rule.rule_id, rule.keyword)

    return RuleResponse(
        rule_id=rule.rule_id,
        keyword=rule.keyword,
        dm_message=rule.dm_message,
    )
