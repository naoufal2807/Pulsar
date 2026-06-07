# Data Exploration Journey: Zero to Ace

## Overview

The **Data Exploration Journey** is a guided learning system that takes users from zero understanding of their dataset to becoming an **"ACE"** - an expert with comprehensive mastery.

Rather than asking random questions, users follow a structured 5-stage journey where an intelligent Agent progressively reveals insights about their data.

```
SCOUT (0%)        EXPLORER (25%)      DETECTIVE (50%)     ANALYST (75%)       ACE (100%)
    |                  |                   |                  |                  |
 Overview         Discovery          Investigation        Advanced          Mastery &
  Basics          Patterns           Anomalies            Insights          Recommendations
```

## The Five Stages

### Stage 1: SCOUT (0%) - Dataset Overview
**Goal:** Understand what the dataset is about

**Questions Explored:**
- What is the shape and size of the dataset?
- What are the columns and their data types?
- What does the data represent?

**Agent Actions:**
- Examines dataset dimensions and schema
- Identifies data types and column purposes
- Provides business context understanding

**Output:** High-level overview of dataset content

---

### Stage 2: EXPLORER (25%) - Pattern Discovery
**Goal:** Discover distributions, patterns, and key columns

**Questions Explored:**
- What are the most important columns?
- What are the distributions like?
- Are there obvious patterns or groupings?

**Agent Actions:**
- Uses `compute_statistics` on numeric columns
- Uses `get_top_values` on categorical columns
- Identifies key metrics and dimensions
- Calls tools to understand data structure

**Output:** Main patterns, distributions, and feature importance

---

### Stage 3: DETECTIVE (50%) - Deep Investigation
**Goal:** Find anomalies, quality issues, and unexpected relationships

**Questions Explored:**
- Are there outliers or anomalies?
- What data quality issues exist?
- Are there unexpected relationships?

**Agent Actions:**
- Uses `detect_outliers` to find anomalies
- Uses `check_data_quality` for null/duplicate analysis
- Investigates suspicious patterns
- Identifies data integrity concerns

**Output:** Anomalies, quality issues, and risks

---

### Stage 4: ANALYST (75%) - Advanced Insights
**Goal:** Understand correlations and strategic drivers

**Questions Explored:**
- What are the key relationships?
- What correlations exist?
- What drives the most important metrics?

**Agent Actions:**
- Uses `analyze_correlation` between variables
- Identifies metric drivers and dependencies
- Explores business implications
- Recognizes strategic insights

**Output:** Correlations, drivers, strategic insights

---

### Stage 5: ACE (100%) - Expert Mastery
**Goal:** Synthesize all learning into expert recommendations

**Questions Explored:**
- What are the top 3 critical findings?
- What actions should be taken?
- What are the risks to monitor?

**Agent Actions:**
- Synthesizes all previous findings
- Generates specific actionable recommendations
- Identifies strategic risks to monitor
- Proposes next steps for deeper analysis

**Output:** Executive summary with recommendations

---

## Usage

### Start Journey at Default (SCOUT)
```bash
pulsar journey netflix_titles.csv
```

Output:
```
================================================================================
[JOURNEY] DATA EXPLORATION JOURNEY: netflix_titles
================================================================================
Your guide: Intelligent Agent
Goal: Become an 'ACE' at understanding your data
Progress: SCOUT (0%) > EXPLORER > DETECTIVE > ANALYST > ACE (100%)
================================================================================

Starting at SCOUT stage...

[Agent analyzes dataset...]

================================================================================
Journey Summary
================================================================================
Dataset: netflix_titles
Size: 8,807 rows × 12 columns
Stage: SCOUT
Progress: 0%
Insights Gained: 2
```

### Start at Specific Stage
```bash
# Start at SCOUT
pulsar journey netflix_titles.csv --stage scout

# Start at EXPLORER
pulsar journey netflix_titles.csv --stage explorer

# Start at DETECTIVE
pulsar journey netflix_titles.csv --stage detective

# Start at ANALYST
pulsar journey netflix_titles.csv --stage analyst

# Start at ACE
pulsar journey netflix_titles.csv --stage ace
```

### Run Complete Journey (SCOUT → ACE)
```bash
pulsar journey netflix_titles.csv --full
```

This runs all 5 stages sequentially with full analysis.

### Save Journey Report
```bash
pulsar journey netflix_titles.csv --full --save
```

Saves detailed report to: `journey_reports/netflix_titles_journey.md`

---

## Journey Progression

Each stage builds on previous stages:

```python
journey = DataExplorationJourney(df, "my_dataset")

# Stage 1
intro = journey.start_journey()  # SCOUT
print(intro)

# Stage 2  
exploration = journey.explore_stage()  # EXPLORER
print(exploration)

# Stage 3
investigation = journey.detective_stage()  # DETECTIVE
print(investigation)

# Stage 4
analysis = journey.analyst_stage()  # ANALYST
print(analysis)

# Stage 5
mastery = journey.ace_stage()  # ACE
print(mastery)

# Get summary
summary = journey.get_journey_summary()
```

---

## Journey Checkpoints

Each stage creates a **JourneyCheckpoint** with:

```python
@dataclass
class JourneyCheckpoint:
    stage: Stage                      # Which stage
    title: str                        # Stage title
    description: str                  # Agent's analysis
    questions_explored: List[str]     # Key questions asked
    insights_gained: List[str]        # Key findings
    next_stage_hint: str              # What's next
```

---

## How Agent Tool-Calling Works

During the journey, the Agent calls tools to analyze your data:

```
User: "Tell me about the netflix_titles dataset"

Agent: "Let me analyze this dataset...
[TOOL: describe_dataset()]
[TOOL: compute_statistics(column=release_year)]
[TOOL: get_top_values(column=country, limit=10)]

Based on the tool results:
- Dataset has 8,807 titles across 12 columns
- Release years range from 1925 to 2021
- Top countries: United States (3,646), India (939), Canada (382)..."
```

Tools used by journey:
- `describe_dataset` - Get schema and size
- `compute_statistics` - Analyze numeric columns
- `check_data_quality` - Check for nulls, duplicates
- `detect_outliers` - Find anomalies
- `analyze_correlation` - Find relationships
- `get_top_values` - Frequency analysis

---

## Example: Complete Journey Output

```
================================================================================
[JOURNEY] DATA EXPLORATION JOURNEY: sales_data
================================================================================
Your guide: Intelligent Agent
Goal: Become an 'ACE' at understanding your data
Progress: SCOUT (0%) > EXPLORER > DETECTIVE > ANALYST > ACE (100%)
================================================================================

Starting at SCOUT stage...

================================================================================
[SCOUT] Dataset Overview
Progress: 0% Complete
================================================================================

This is a sales dataset containing 10,000 records across 8 columns...
[Agent analysis of dataset overview]

[INSIGHTS] Key Insights This Stage:
  • Dataset contains 10,000 rows and 8 columns
  • Identified 8 distinct data fields

→ Next: Let's EXPLORE the patterns and distributions in the data.

================================================================================
[EXPLORER] Pattern Discovery
Progress: 25% Complete
================================================================================

The dataset shows clear patterns in sales by region...
[Agent analysis of patterns and distributions]

[INSIGHTS] Key Insights This Stage:
  • Identified 5 numeric columns
  • Found 3 categorical columns
  • Discovered main data distributions

→ Next: Let's DETECT anomalies and deep patterns in the data.

... [continues through DETECTIVE, ANALYST, ACE stages] ...

================================================================================
Journey Summary
================================================================================
Dataset: sales_data
Size: 10,000 rows × 8 columns
Stage: ACE
Progress: 100%
Insights Gained: 15
```

---

## Use Cases

### 1. Onboard New Team Members
Guide them through structured understanding of company data:
```bash
pulsar journey customer_data.csv --full
```

### 2. Audit Data Quality
Detect issues systematically:
```bash
pulsar journey data_export.csv --stage detective --save
```

### 3. Prepare for Analysis
Understand data before building models:
```bash
pulsar journey training_data.csv --stage explorer
```

### 4. Executive Summary
Generate recommendations:
```bash
pulsar journey quarterly_results.csv --stage ace
```

---

## Architecture

```python
DataExplorationJourney
├── __init__(df, dataset_name)
├── Agent(df, tools_enabled=True)  # With 6 tools
├── Stages:
│   ├── start_journey() → SCOUT checkpoint
│   ├── explore_stage() → EXPLORER checkpoint
│   ├── detective_stage() → DETECTIVE checkpoint
│   ├── analyst_stage() → ANALYST checkpoint
│   └── ace_stage() → ACE checkpoint
├── run_complete_journey() → All stages
└── get_journey_summary() → Summary dict
```

---

## Progress Tracking

Monitor how far user has progressed:

```python
summary = journey.get_journey_summary()
print(f"Stage: {summary['current_stage']}")      # "ace"
print(f"Progress: {summary['progress_percent']}%") # 100
print(f"Insights: {summary['total_insights']}")  # 20
```

---

## Next Steps After Journey

After completing the journey, users can:

1. **Deep Dive on Specific Topics**
   - Use Agent to analyze specific columns
   - Explore relationships in detail

2. **Generate Detailed Reports**
   - Create visualizations of findings
   - Document insights for stakeholders

3. **Monitor Data Over Time**
   - Establish baseline understanding
   - Track how data changes

4. **Build Models with Confidence**
   - Understand data quality
   - Know key features and relationships

---

## Benefits

✅ **Structured Learning** - Clear progression from basics to mastery
✅ **Comprehensive** - Covers overview, patterns, quality, relationships, insights
✅ **Automated** - Agent handles analysis, user focuses on learning
✅ **Guided** - Agent asks the right questions at each stage
✅ **Tool-Powered** - Uses actual data analysis tools, not just text
✅ **Reproducible** - Same journey on any dataset
✅ **Executive Ready** - Produces actionable recommendations

---

## Test Coverage

- 14 tests covering all stages
- Tests for progression, checkpoints, summaries
- All tests passing

---

## Related Documents

- [Agent Capabilities](AGENT_CAPABILITIES.md) - How the Agent works
- [Tool System](AGENT_CAPABILITIES.md#tool-calling-capabilities) - Available tools
- CLI Reference: `pulsar journey --help`
