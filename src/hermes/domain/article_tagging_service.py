import logging
from typing import Dict, List, Optional
from sqlalchemy.orm import Session, joinedload
from hermes.domain.database import ArticleCard, ArticleTag, ArticleDescription
from hermes.core.cleaning_by_context import search_insertion_context, compare_prefix, levenshtein

logger = logging.getLogger(__name__)

class ArticleTaggingService:
    def __init__(self, session: Session):
        self.session = session
        self.tag_cache: Dict[str, ArticleTag] = {}

    def get_or_create_tag(self, tag_text: str) -> ArticleTag:
        """
        Efficiently gets a tag from cache, DB, or creates a new one.
        """
        if tag_text in self.tag_cache:
            return self.tag_cache[tag_text]

        tag = self.session.query(ArticleTag).filter_by(tag=tag_text).first()
        if tag:
            self.tag_cache[tag_text] = tag
            return tag

        new_tag = ArticleTag(tag=tag_text)
        self.session.add(new_tag)
        self.tag_cache[tag_text] = new_tag
        return new_tag

    def generate_high_confidence_tags(self) -> None:
        logger.info("Phase 1: Generating high-confidence tags...")
        all_cards = self.session.query(ArticleCard).all()
        for i, card in enumerate(all_cards):
            brand = card.brand.brand
            description = card.description.description

            parts = description.split(brand)
            if len(parts) == 2 and all(parts):
                tag_text = parts[0].strip()
                if tag_text:
                    tag = self.get_or_create_tag(tag_text)
                    if tag not in card.tags:
                        card.tags.append(tag)

            if (i + 1) % 500 == 0:
                logger.info(f"Phase 1: Committing batch, processed {i+1}/{len(all_cards)} cards.")
                self.session.commit()
        
        self.session.commit()
        logger.info("Phase 1 complete.")

    def clean_and_match_remaining(self) -> None:
        logger.info("Phase 2: Cleaning and matching remaining cards...")
        all_tag_strings = sorted([t.tag for t in self.session.query(ArticleTag).all()])
        untagged_cards = self.session.query(ArticleCard).filter(~ArticleCard.tags.any()).all()
        
        for i, card in enumerate(untagged_cards):
            description = card.description.description
            before, after = search_insertion_context(all_tag_strings, description)
            max_len = max(len(before) if before else 0, len(after) if after else 0, len(description) if description else 0)
            
            lvs_before = levenshtein(before, description) if before else len(description)
            lvs_after = levenshtein(after, description) if after else len(description)

            min_lvs = min(lvs_before, lvs_after)
            best_match: Optional[str] = None
            
            if min_lvs == 0:
                best_match = description
            else:
                candidate = before if lvs_before == min_lvs else after
                if candidate and min_lvs * 5 > max_len:
                    before_prefix_len = compare_prefix(before, description) if before else 0
                    after_prefix_len = compare_prefix(after, description) if after else 0
                    best_prefix_len = max(before_prefix_len, after_prefix_len)
                    
                    if best_prefix_len > min_lvs:
                        best_match = description[:best_prefix_len]
            
            if best_match:
                tag = self.get_or_create_tag(best_match)
                if tag not in card.tags:
                    card.tags.append(tag)

            if (i + 1) % 500 == 0:
                logger.info(f"Phase 2: Committing batch, processed {i+1}/{len(untagged_cards)} cards.")
                self.session.commit()

        self.session.commit()
        logger.info("Phase 2 complete.")

    def process_rogue_cards(self) -> None:
        logger.info("Phase 3: Processing rogue cards...")
        rogue_cards = (
            self.session.query(ArticleCard)
            .join(ArticleCard.description)
            .options(
                joinedload(ArticleCard.brand),
                joinedload(ArticleCard.description),
                joinedload(ArticleCard.package),
                joinedload(ArticleCard.code),
            )
            .filter(~ArticleCard.tags.any())
            .order_by(ArticleDescription.description)
            .all()
        )
        
        if not rogue_cards:
            logger.info("No rogue cards found.")
            return

        former_card = rogue_cards[0]
        for i, current_card in enumerate(rogue_cards[1:], 1):
            if former_card.brand.id == current_card.brand.id:
                former_description = former_card.description.description
                current_description = current_card.description.description
                prefix_size = compare_prefix(former_description, current_description)
                
                if prefix_size > 5:
                    prefix = (current_description[:prefix_size]).strip()
                    tag = self.get_or_create_tag(prefix)
                    if tag not in current_card.tags:
                        current_card.tags.append(tag)
                    if tag not in former_card.tags:
                        former_card.tags.append(tag)

            if (i + 1) % 500 == 0:
                logger.info(f"Phase 3: Committing batch, processed {i+1}/{len(rogue_cards)} cards.")
                self.session.commit()
            former_card = current_card

        self.session.commit()
        logger.info("Phase 3 complete.")
