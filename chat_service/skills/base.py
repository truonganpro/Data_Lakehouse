# -*- coding: utf-8 -*-
"""
Base skill class for SQL generation
"""
from typing import Dict, Optional
from abc import ABC, abstractmethod


class BaseSkill(ABC):
    """Base class for all SQL skills"""
    
    def __init__(self):
        self.name = self.__class__.__name__
        self.priority = 50  # Higher = higher priority
    
    @abstractmethod
    def match(self, question: str, entities: Dict) -> float:
        """
        Check if this skill matches the question
        
        Args:
            question: User's question
            entities: Extracted entities from NLP
            
        Returns:
            Confidence score (0.0 - 1.0)
        """
        pass
    
    @abstractmethod
    def render(self, question: str, params: Dict) -> str:
        """
        Render SQL query from parameters
        
        Args:
            question: Original question
            params: Parameters dict (time_window, topn, entities, etc.)
            
        Returns:
            SQL query string
        """
        pass
    
    def get_metadata(self) -> Dict:
        """Get skill metadata"""
        return {
            'name': self.name,
            'priority': self.priority,
            'description': self.__doc__ or ''
        }

