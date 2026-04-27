# pulsar/output/formatter.py

import json
from typing import Dict, Any


def format_validation_output(
    results: Dict[str, Dict[str, Any]],
    output: str = "text",
    verbose: bool = False
) -> str:
    """Format validation results."""
    
    if output == "json":
        return _format_json(results, verbose)
    else:
        return _format_text(results, verbose)


def _format_text(results: Dict[str, Dict[str, Any]], verbose: bool) -> str:
    """Format as text table."""
    if not results:
        return "No validation results"
    
    lines = []
    lines.append("\n" + "="*70)
    lines.append("VALIDATION RESULTS")
    lines.append("="*70)
    
    passed_count = 0
    for rule_name, result in results.items():
        status = result.get("status", "UNKNOWN")
        percentage = result.get("percentage", 0)
        
        if status == "PASS":
            icon = "✅"
            passed_count += 1
        elif status == "FAIL":
            icon = "❌"
        else:
            icon = "⚠️ "
        
        line = f"{icon} {rule_name:30} {status:6} {percentage:6.1f}%"
        lines.append(line)
        
        if verbose and status == "FAIL":
            if "error" in result:
                lines.append(f"   Error: {result['error']}")
            else:
                passed = result.get("passed", 0)
                total = result.get("total", 0)
                lines.append(f"   ({passed}/{total} rows passed)")
    
    lines.append("-"*70)
    total = len(results)
    lines.append(f"Summary: {passed_count}/{total} rules passed ({(passed_count/total*100):.1f}%)")
    lines.append("="*70 + "\n")
    
    return "\n".join(lines)


def _format_json(results: Dict[str, Dict[str, Any]], verbose: bool) -> str:
    """Format as JSON."""
    passed_count = sum(1 for r in results.values() if r.get("status") == "PASS")
    total = len(results)
    
    output = {
        "validation_results": results,
        "summary": {
            "total_rules": total,
            "passed": passed_count,
            "failed": total - passed_count,
            "pass_rate": (passed_count / total * 100) if total > 0 else 0
        }
    }
    
    return json.dumps(output, indent=2)
