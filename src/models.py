# models.py
"""Data models for the supplement analyzer"""
from dataclasses import dataclass, asdict
from typing import Optional, List, Dict, Any
from datetime import datetime
import json


@dataclass
class Study:
    """Represents a research study"""
    id: int
    supplement_id: int
    supplement_name: str
    pmid: Optional[str]
    title: Optional[str]
    abstract: Optional[str]
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class AnalysisResult:
    """Represents the analysis result for a study"""
    study_id: int
    supplement_id: int
    safety_score: Optional[str]
    efficacy_score: Optional[int]
    quality_score: Optional[int]
    study_goal: Optional[str]
    results_summary: Optional[str]
    population_specificity: Optional[str]
    effective_dosage: Optional[str]
    study_duration: Optional[str]
    interactions: Optional[str]
    analysis_prompt_version: str
    last_analyzed_at: str
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    def to_json_line(self) -> str:
        """Convert to JSON line for JSONL storage"""
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AnalysisResult':
        return cls(**data)


@dataclass
class WorkBatch:
    """Represents a batch of work"""
    batch_id: int
    studies: List[Study]
    created_at: datetime
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'batch_id': self.batch_id,
            'studies': [s.to_dict() for s in self.studies],
            'created_at': self.created_at.isoformat()
        }


@dataclass
class Progress:
    """Tracks overall progress"""
    total_papers: int
    processed_papers: int
    failed_papers: int
    current_batch_id: int
    batches_completed: List[int]
    start_time: datetime
    last_update: datetime
    error_counts: Dict[str, int]
    
    @property
    def success_rate(self) -> float:
        if self.processed_papers == 0:
            return 0.0
        return (self.processed_papers - self.failed_papers) / self.processed_papers
    
    @property
    def papers_per_second(self) -> float:
        elapsed = (self.last_update - self.start_time).total_seconds()
        if elapsed == 0:
            return 0.0
        return self.processed_papers / elapsed
    
    @property
    def estimated_time_remaining(self) -> float:
        if self.papers_per_second == 0:
            return float('inf')
        remaining = self.total_papers - self.processed_papers
        return remaining / self.papers_per_second
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'total_papers': self.total_papers,
            'processed_papers': self.processed_papers,
            'failed_papers': self.failed_papers,
            'current_batch_id': self.current_batch_id,
            'batches_completed': self.batches_completed,
            'start_time': self.start_time.isoformat(),
            'last_update': self.last_update.isoformat(),
            'error_counts': self.error_counts,
            'success_rate': self.success_rate,
            'papers_per_second': self.papers_per_second,
            'estimated_time_remaining_seconds': self.estimated_time_remaining
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Progress':
        data['start_time'] = datetime.fromisoformat(data['start_time'])
        data['last_update'] = datetime.fromisoformat(data['last_update'])
        # Remove computed properties if present
        data.pop('success_rate', None)
        data.pop('papers_per_second', None)
        data.pop('estimated_time_remaining_seconds', None)
        return cls(**data)


@dataclass
class FailedPaper:
    """Represents a failed paper for retry"""
    study: Study
    error: str
    error_type: str
    attempts: int
    last_attempt: datetime
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'study': self.study.to_dict(),
            'error': self.error,
            'error_type': self.error_type,
            'attempts': self.attempts,
            'last_attempt': self.last_attempt.isoformat()
        }
    
    def to_json_line(self) -> str:
        return json.dumps(self.to_dict())