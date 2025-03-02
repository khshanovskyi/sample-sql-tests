-- Test data for SQL query testing

-- Insert departments
INSERT INTO departments (name, location) VALUES
                                             ('Engineering', 'Building A'),
                                             ('Marketing', 'Building B'),
                                             ('Finance', 'Building C'),
                                             ('Human Resources', 'Building B');

-- Insert employees
INSERT INTO employees (first_name, last_name, email, salary, hire_date, department_id, active) VALUES
                                                                                                   ('John', 'Smith', 'john.smith@example.com', 75000.00, '2019-04-15', 1, TRUE),
                                                                                                   ('Sarah', 'Johnson', 'sarah.j@example.com', 82000.00, '2018-06-23', 1, TRUE),
                                                                                                   ('Michael', 'Williams', 'michael.w@example.com', 67500.00, '2020-01-10', 2, TRUE),
                                                                                                   ('Emily', 'Brown', 'emily.b@example.com', 72000.00, '2019-11-05', 2, TRUE),
                                                                                                   ('David', 'Jones', 'david.j@example.com', 91000.00, '2017-03-20', 3, TRUE),
                                                                                                   ('Jennifer', 'Miller', 'jennifer.m@example.com', 68000.00, '2021-02-28', 4, TRUE),
                                                                                                   ('Robert', 'Davis', 'robert.d@example.com', 78500.00, '2018-09-15', 1, FALSE),
                                                                                                   ('Jessica', 'Garcia', 'jessica.g@example.com', 81000.00, '2019-07-10', 3, TRUE);

-- Insert projects
INSERT INTO projects (name, start_date, end_date, budget, status) VALUES
                                                                      ('Database Migration', '2023-01-15', '2023-06-30', 150000.00, 'IN_PROGRESS'),
                                                                      ('Website Redesign', '2023-02-10', '2023-08-15', 95000.00, 'IN_PROGRESS'),
                                                                      ('Mobile App Development', '2023-03-01', '2023-12-31', 275000.00, 'PLANNING'),
                                                                      ('Annual Audit', '2023-04-01', '2023-05-15', 45000.00, 'COMPLETED'),
                                                                      ('Marketing Campaign', '2023-05-10', '2023-07-31', 85000.00, 'IN_PROGRESS');

-- Assign employees to projects
INSERT INTO employee_projects (employee_id, project_id, role) VALUES
                                                                  (1, 1, 'Lead Developer'),
                                                                  (2, 1, 'Database Architect'),
                                                                  (7, 1, 'Developer'),
                                                                  (3, 2, 'Content Specialist'),
                                                                  (4, 2, 'Designer'),
                                                                  (1, 3, 'Technical Advisor'),
                                                                  (2, 3, 'Developer'),
                                                                  (7, 3, 'Developer'),
                                                                  (5, 4, 'Project Manager'),
                                                                  (8, 4, 'Financial Analyst'),
                                                                  (3, 5, 'Marketing Specialist'),
                                                                  (4, 5, 'Creative Director');