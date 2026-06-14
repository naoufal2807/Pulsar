"""Prompt system with template loading and variable injection."""

from typing import Dict, Any, Optional
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class PromptTemplate:
    """Prompt template with variable injection."""

    def __init__(self, name: str, content: str):
        """Initialize prompt template."""
        self.name = name
        self.content = content

    def render(self, **variables) -> str:
        """
        Render template with variables.

        Args:
            **variables: Variables to inject into template

        Returns:
            Rendered prompt string
        """
        try:
            return self.content.format(**variables)
        except KeyError as e:
            logger.warning(f"Missing variable in template {self.name}: {e}")
            # Try to fill with empty string for missing vars
            import re
            missing_vars = re.findall(r'\{(\w+)\}', self.content)
            for var in missing_vars:
                if var not in variables:
                    variables[var] = ''
            return self.content.format(**variables)


class PromptSystem:
    """System for managing prompts and templates."""

    def __init__(self, templates_dir: Optional[Path] = None):
        """
        Initialize prompt system.

        Args:
            templates_dir: Directory containing prompt templates
        """
        if templates_dir is None:
            templates_dir = Path(__file__).parent / 'prompts'

        self.templates_dir = templates_dir
        self.templates: Dict[str, PromptTemplate] = {}
        self._load_templates()

    def _load_templates(self) -> None:
        """Load all prompt templates from directory."""
        if not self.templates_dir.exists():
            logger.warning(f"Templates directory not found: {self.templates_dir}")
            return

        for template_file in self.templates_dir.glob('*.txt'):
            try:
                content = template_file.read_text()
                template_name = template_file.stem
                self.templates[template_name] = PromptTemplate(template_name, content)
                logger.debug(f"Loaded template: {template_name}")
            except Exception as e:
                logger.error(f"Failed to load template {template_file}: {e}")

    def get_template(self, name: str) -> Optional[PromptTemplate]:
        """Get a template by name."""
        return self.templates.get(name)

    def render(self, template_name: str, **variables) -> str:
        """
        Render a template with variables.

        Args:
            template_name: Name of template (without .txt extension)
            **variables: Variables to inject

        Returns:
            Rendered prompt

        Raises:
            ValueError: If template not found
        """
        template = self.get_template(template_name)
        if not template:
            raise ValueError(f"Template not found: {template_name}")
        return template.render(**variables)

    def render_system_prompt(
        self,
        domain: str,
        dataset_name: str,
        analysis_purpose: str = "Comprehensive data analysis"
    ) -> str:
        """Render system prompt with variables."""
        return self.render(
            'system_base',
            domain=domain,
            dataset_name=dataset_name,
            analysis_purpose=analysis_purpose
        )

    def render_journey_stage(
        self,
        stage: str,
        dataset_name: str,
        domain: str,
        row_count: Optional[int] = None,
        column_count: Optional[int] = None,
        column_names: Optional[str] = None
    ) -> str:
        """
        Render journey stage prompt.

        Args:
            stage: Journey stage name (scout, explorer, detective, analyst, ace)
            dataset_name: Name of dataset
            domain: Business domain
            row_count: Number of rows
            column_count: Number of columns
            column_names: Comma-separated column names

        Returns:
            Rendered journey prompt
        """
        template_name = f'journey_{stage}'
        return self.render(
            template_name,
            dataset_name=dataset_name,
            domain=domain,
            row_count=row_count or '?',
            column_count=column_count or '?',
            column_names=column_names or '?'
        )


# Global prompt system instance
_prompt_system: Optional[PromptSystem] = None


def get_prompt_system() -> PromptSystem:
    """Get or create global prompt system."""
    global _prompt_system
    if _prompt_system is None:
        _prompt_system = PromptSystem()
    return _prompt_system
