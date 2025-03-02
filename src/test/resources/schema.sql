-- Database schema for testing SQL queries

-- Create departments table
CREATE TABLE departments (
                             id SERIAL PRIMARY KEY,
                             name VARCHAR(100) NOT NULL,
                             location VARCHAR(100),
                             created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create employees table
CREATE TABLE employees (
                           id SERIAL PRIMARY KEY,
                           first_name VARCHAR(50) NOT NULL,
                           last_name VARCHAR(50) NOT NULL,
                           email VARCHAR(100) UNIQUE,
                           salary NUMERIC(10, 2),
                           hire_date DATE NOT NULL,
                           department_id INTEGER REFERENCES departments(id),
                           active BOOLEAN DEFAULT TRUE
);

-- Create projects table
CREATE TABLE projects (
                          id SERIAL PRIMARY KEY,
                          name VARCHAR(100) NOT NULL,
                          start_date DATE,
                          end_date DATE,
                          budget NUMERIC(14, 2),
                          status VARCHAR(20) DEFAULT 'PLANNING'
);

-- Create employee_projects (many-to-many) table
CREATE TABLE employee_projects (
                                   employee_id INTEGER REFERENCES employees(id),
                                   project_id INTEGER REFERENCES projects(id),
                                   role VARCHAR(50),
                                   assignment_date DATE DEFAULT CURRENT_DATE,
                                   PRIMARY KEY (employee_id, project_id)
);