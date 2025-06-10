# processor.py
"""DeepSeek API processor for analyzing abstracts"""
import asyncio
import aiohttp
import json
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone
import backoff

from src.config import (
    DEEPSEEK_API_KEY, DEEPSEEK_BASE_URL, DEEPSEEK_MODEL,
    API_MAX_RETRIES, API_RETRY_DELAY, API_RETRY_BACKOFF,
    API_TIMEOUT, TEMPERATURE, PROMPT_VERSION
)
from src.models import Study, AnalysisResult

logger = logging.getLogger(__name__)


class ProcessorError(Exception):
    """Custom processor exception"""
    pass


class DeepSeekProcessor:
    """Handles DeepSeek API calls for abstract analysis"""
    
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {DEEPSEEK_API_KEY}"
        }
        self.url = f"{DEEPSEEK_BASE_URL}/v1/chat/completions"
    
    def _get_system_message(self) -> str:
        """Get the system message for the API"""
        return """You are a scientific research analysis expert. Follow these instructions carefully:
        
        1. When asked to rate on a scale of 1-10, ALWAYS respond with a single integer between 1 and 10.
        2. If information is not available to assess a category, respond ONLY with "Not Assessed" (no other explanation).
        3. For safety, efficacy, and quality scores, you must provide either a single integer (1-10) or "Not Assessed".
        4. Do not include explanations within the score fields.
        5. Format your response exactly as requested in the prompt."""
    
    def _get_analysis_prompt(self, supplement_name: str, abstract_text: str) -> str:
        """Get the analysis prompt for a specific abstract"""
        return f"""You are to perform an analysis of the research paper abstract provided below, which relates to {supplement_name}. Output only the specified information and nothing else. Format your response exactly as requested.

Abstract to analyze:
---
{abstract_text}
---

Based on the abstract above, provide the following information:

- Safety Score (1-10): Evaluate whether the study demonstrates that {supplement_name} is safe for human consumption. Consider reported adverse events, side effects, toxicity information, contraindications, and safety profiles across different dosages. A score of 1 indicates significant safety concerns, while 10 indicates excellent safety with no reported adverse effects. If safety is not addressed in the study, mark as "Not Assessed."

- Efficacy Score (1-10): Rate how effectively the supplement achieved its intended outcomes based on the study results. Consider statistical significance, effect size, clinical relevance, and consistency of results. A score of 1 indicates no demonstrable effect, 5 indicates modest effects, and 10 indicates strong, clinically meaningful outcomes. If the study shows mixed results, explain the context.

- Study Quality Score (1-10): Evaluate the methodological rigor of the study based on:
  * Study design (RCT > cohort > case-control > case series)
  * Sample size (larger samples receive higher scores)
  * Appropriate controls and blinding procedures
  * Statistical analysis methods
  * Duration of follow-up
  * Funding source independence
  * Peer-review status and journal reputation

Scoring Guidelines:
When assigning scores from 1-10, please use the full range of the scale. Avoid clustering scores in the middle range (4-7) simply to appear moderate. Each score should accurately reflect the paper's merits on that dimension, even if that means giving very high (9-10) or very low (1-2) scores when warranted.

- Study Goal: In 1-2 sentences, describe what the researchers were attempting to determine about {supplement_name}.

- Results Summary: In 2-3 sentences, concisely describe what the study found regarding {supplement_name}'s effects.

- Population Specificity: Note the specific population studied.

- Effective Dosage: If provided, note the dosage(s) used in the study.

- Study Duration: How long was the intervention administered?

- Interactions: Note any mentioned interactions with medications, foods, or other supplements.

Format your response as follows:

SAFETY SCORE: [1-10 or "Not Assessed"]
EFFICACY SCORE: [1-10]
QUALITY SCORE: [1-10]
GOAL: [1-2 sentences]
RESULTS: [2-3 sentences]
POPULATION: [Brief description]
DOSAGE: [Amount and frequency, if available]
DURATION: [Length of intervention]
INTERACTIONS: [Any noted interactions or "None mentioned"]"""
    
    @backoff.on_exception(
        backoff.expo,
        (aiohttp.ClientError, asyncio.TimeoutError),
        max_tries=API_MAX_RETRIES,
        base=API_RETRY_DELAY,
        factor=API_RETRY_BACKOFF
    )
    async def analyze_study(self, study: Study) -> AnalysisResult:
        """Analyze a single study using DeepSeek API"""
        if not study.abstract:
            raise ProcessorError("No abstract available for analysis")
        
        payload = {
            "model": DEEPSEEK_MODEL,
            "messages": [
                {"role": "system", "content": self._get_system_message()},
                {"role": "user", "content": self._get_analysis_prompt(study.supplement_name, study.abstract)}
            ],
            "temperature": TEMPERATURE,
        }
        
        try:
            async with self.session.post(
                self.url,
                headers=self.headers,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=API_TIMEOUT)
            ) as response:
                if response.status != 200:
                    text = await response.text()
                    raise ProcessorError(f"API returned status {response.status}: {text}")
                
                data = await response.json()
                result_text = data["choices"][0]["message"]["content"]
                
                # Parse the response
                parsed = self._parse_response(result_text)
                
                # Create AnalysisResult
                return AnalysisResult(
                    study_id=study.id,
                    supplement_id=study.supplement_id,
                    safety_score=parsed.get('safety_score'),
                    efficacy_score=parsed.get('efficacy_score'),
                    quality_score=parsed.get('quality_score'),
                    study_goal=parsed.get('study_goal'),
                    results_summary=parsed.get('results_summary'),
                    population_specificity=parsed.get('population_specificity'),
                    effective_dosage=parsed.get('effective_dosage'),
                    study_duration=parsed.get('study_duration'),
                    interactions=parsed.get('interactions'),
                    analysis_prompt_version=PROMPT_VERSION,
                    last_analyzed_at=datetime.now(timezone.utc).isoformat()
                )
                
        except asyncio.TimeoutError:
            raise ProcessorError(f"API timeout after {API_TIMEOUT} seconds")
        except Exception as e:
            raise ProcessorError(f"API error: {str(e)}")
    
    def _parse_response(self, response_text: str) -> Dict[str, Any]:
        """Parse the API response to extract structured data"""
        results = {}
        
        # Map of response fields to result keys
        fields_map = {
            'SAFETY SCORE:': 'safety_score',
            'EFFICACY SCORE:': 'efficacy_score',
            'QUALITY SCORE:': 'quality_score',
            'GOAL:': 'study_goal',
            'RESULTS:': 'results_summary',
            'POPULATION:': 'population_specificity',
            'DOSAGE:': 'effective_dosage',
            'DURATION:': 'study_duration',
            'INTERACTIONS:': 'interactions'
        }
        
        lines = response_text.strip().split('\n')
        
        for line in lines:
            for prefix, field in fields_map.items():
                if line.startswith(prefix):
                    value = line.replace(prefix, '').strip()
                    
                    # Handle numeric fields
                    if field in ['efficacy_score', 'quality_score']:
                        if value and value.isdigit():
                            value = int(value)
                        elif value and "not assessed" in value.lower():
                            value = None
                        else:
                            # Try to extract a number if present
                            for part in value.split():
                                if part.isdigit():
                                    value = int(part)
                                    break
                            else:
                                value = None
                    
                    # Handle safety score (can be "Not Assessed")
                    elif field == 'safety_score':
                        if value and "not assessed" in value.lower():
                            value = "Not Assessed"
                    
                    results[field] = value
                    break
        
        return results


class ProcessorPool:
    """Manages a pool of processors for concurrent API calls"""
    
    def __init__(self, pool_size: int):
        self.pool_size = pool_size
        self.session: Optional[aiohttp.ClientSession] = None
        self.processors: List[DeepSeekProcessor] = []
        self.semaphore = asyncio.Semaphore(pool_size)
        self.processor_index = 0
    
    async def initialize(self):
        """Initialize the processor pool"""
        # Create a single session with connection pooling
        connector = aiohttp.TCPConnector(
            limit=self.pool_size,
            limit_per_host=self.pool_size
        )
        self.session = aiohttp.ClientSession(connector=connector)
        
        # Create processors
        for _ in range(self.pool_size):
            processor = DeepSeekProcessor(self.session)
            self.processors.append(processor)
        
        logger.info(f"Initialized processor pool with {self.pool_size} processors")
    
    async def process_study(self, study: Study) -> AnalysisResult:
        """Process a study using an available processor"""
        async with self.semaphore:
            # Get next available processor (round-robin)
            processor = self.processors[self.processor_index]
            self.processor_index = (self.processor_index + 1) % self.pool_size
            
            return await processor.analyze_study(study)
    
    async def close(self):
        """Close the processor pool"""
        if self.session:
            await self.session.close()
            # Wait a bit for connections to close properly
            await asyncio.sleep(0.25)