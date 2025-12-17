-- Query 1: Revenue by Segment and Region
-- Helps identify high-value geographic markets per business segment.
SELECT 
    segment, 
    region, 
    COUNT(id) as client_count, 
    SUM(revenue) as total_revenue, 
    AVG(lead_score) as avg_health_score
FROM crm_analytics_ready
GROUP BY segment, region
ORDER BY total_revenue DESC;

-- Query 2: Sales Rep Performance (Conversion Rates)
-- Analyzes which owners are closing the most revenue vs total assigned deals.
SELECT 
    owner,
    COUNT(CASE WHEN deal_stage = 'Closed Won' THEN 1 END) as deals_won,
    COUNT(id) as total_assigned,
    (COUNT(CASE WHEN deal_stage = 'Closed Won' THEN 1 END) * 100.0 / COUNT(id)) as win_rate_percentage,
    SUM(CASE WHEN deal_stage = 'Closed Won' THEN revenue ELSE 0 END) as revenue_booked
FROM crm_analytics_ready
WHERE owner != 'Unassigned'
GROUP BY owner
ORDER BY revenue_booked DESC;

-- Query 3: Deal Stage Funnel Analysis
-- Shows the distribution of potential revenue across pipeline stages.
SELECT 
    deal_stage,
    COUNT(id) as deal_count,
    SUM(revenue) as pipeline_value,
    AVG(employee_count) as avg_company_size
FROM crm_analytics_ready
GROUP BY deal_stage
ORDER BY pipeline_value DESC;