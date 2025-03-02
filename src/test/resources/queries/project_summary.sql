-- Query to get project summaries with employee count and budget information
SELECT
    p.id AS project_id,
    p.name AS project_name,
    p.status,
    p.start_date,
    p.end_date,
    p.budget,
    COUNT(ep.employee_id) AS employee_count,
    ROUND(p.budget / NULLIF(COUNT(ep.employee_id), 0), 2) AS budget_per_employee
FROM
    projects p
        LEFT JOIN
    employee_projects ep ON p.id = ep.project_id
GROUP BY
    p.id, p.name, p.status, p.start_date, p.end_date, p.budget
ORDER BY
    p.start_date;