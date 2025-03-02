-- Query to calculate salary statistics by department
SELECT
    d.id AS department_id,
    d.name AS department_name,
    COUNT(e.id) AS employee_count,
    ROUND(AVG(e.salary), 2) AS avg_salary,
    MIN(e.salary) AS min_salary,
    MAX(e.salary) AS max_salary,
    SUM(e.salary) AS total_salary_expense
FROM
    departments d
        LEFT JOIN
    employees e ON d.id = e.department_id AND e.active = TRUE
GROUP BY
    d.id, d.name
ORDER BY
    avg_salary DESC;