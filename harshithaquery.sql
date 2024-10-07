
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    address VARCHAR(100),
    city VARCHAR(50),
    state VARCHAR(50),
    zip VARCHAR(20)
);


CREATE TABLE accounts (
    account_id INT PRIMARY KEY,
    customer_id INT,
    account_type VARCHAR(50),
    balance DECIMAL(10, 2),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);


CREATE TABLE transactions (
    transaction_id INT PRIMARY KEY,
    account_id INT,
    transaction_date DATE,
    transaction_amount DECIMAL(10, 2),
    transaction_type VARCHAR(50),
    FOREIGN KEY (account_id) REFERENCES accounts(account_id)
);


CREATE TABLE loans (
    loan_id INT PRIMARY KEY,
    customer_id INT,
    loan_amount DECIMAL(10, 2),
    interest_rate DECIMAL(5, 2),
    loan_term INT,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);


CREATE TABLE loan_payments (
    payment_id INT PRIMARY KEY,
    loan_id INT,
    payment_date DATE,
    payment_amount DECIMAL(10, 2),
    FOREIGN KEY (loan_id) REFERENCES loans(loan_id)
);

INSERT INTO customers (customer_id, first_name, last_name, address, city, state, zip) VALUES
(1, 'Rajesh', 'Sharma', '123 MG Road', 'Mumbai', 'Maharashtra', '400001'),
(2, 'Anjali', 'Verma', '456 Sector 5', 'Gurgaon', 'Haryana', '122001'),
(3, 'Ravi', 'Kumar', '789 Green Park', 'Delhi', 'Delhi', '110016'),
(4, 'Priya', 'Mehta', '321 Ring Road', 'Ahmedabad', 'Gujarat', '380001'),
(5, 'Arjun', 'Desai', '654 Hill View', 'Bangalore', 'Karnataka', '560001'),
(6, 'Sneha', 'Gupta', '111 Park Street', 'Kolkata', 'West Bengal', '700001'),
(7, 'Mohan', 'Reddy', '222 Jubilee Hills', 'Hyderabad', 'Telangana', '500033'),
(8, 'Neha', 'Patel', '333 SG Highway', 'Surat', 'Gujarat', '395007'),
(9, 'Karan', 'Malhotra', '444 Marine Drive', 'Mumbai', 'Maharashtra', '400002'),
(10, 'Aditi', 'Nair', '555 Palm Beach Road', 'Navi Mumbai', 'Maharashtra', '400703');


--Insert into accounts Table:
INSERT INTO accounts (account_id, customer_id, account_type, balance) VALUES
(1, 1, 'Savings', 50000.00),
(2, 2, 'Current', 150000.00),
(3, 3, 'Savings', 75000.00),
(4, 4, 'Current', 50000.00),
(5, 5, 'Savings', 200000.00),
(6, 6, 'Savings', 100000.00),
(7, 7, 'Current', 30000.00),
(8, 8, 'Savings', 120000.00),
(9, 9, 'Current', 95000.00),
(10, 10, 'Savings', 80000.00);



-- Insert into transactions Table:
INSERT INTO transactions (transaction_id, account_id, transaction_date, transaction_amount, transaction_type) VALUES
(1, 1, '2024-03-01', 5000.00, 'Credit'),
(2, 2, '2024-09-02', 20000.00, 'Debit'),
(3, 3, '2024-09-03', 10000.00, 'Credit'),
(4, 4, '2024-03-04', 5000.00, 'Debit'),
(5, 5, '2024-09-05', 50000.00, 'Credit'),
(6, 6, '2024-09-06', 20000.00, 'Debit'),
(7, 7, '2024-09-07', 1000.00, 'Credit'),
(8, 8, '2024-09-08', 30000.00, 'Debit'),
(9, 9, '2024-09-09', 5000.00, 'Credit'),
(10, 10, '2024-09-10', 10000.00, 'Debit');

	

--Insert into loans Table:
INSERT INTO loans (loan_id, customer_id, loan_amount, interest_rate, loan_term) VALUES
(1, 1, 100000.00, 8.5, 60),
(2, 2, 200000.00, 9.0, 72),
(3, 3, 150000.00, 8.0, 48),
(4, 4, 250000.00, 7.5, 84),
(5, 5, 300000.00, 9.2, 60),
(6, 6, 120000.00, 8.8, 36),
(7, 7, 180000.00, 7.9, 48),
(8, 8, 220000.00, 9.1, 60),
(9, 9, 160000.00, 8.3, 72),
(10, 10, 190000.00, 8.6, 60);


--Insert into loan_payments Table:
INSERT INTO loan_payments (payment_id, loan_id, payment_date, payment_amount) VALUES
(1, 1, '2024-09-11', 2000.00),
(2, 2, '2024-09-12', 3000.00),
(3, 3, '2024-09-13', 2500.00),
(4, 4, '2024-09-14', 4000.00),
(5, 5, '2024-09-15', 5000.00),
(6, 6, '2024-09-16', 1500.00),
(7, 7, '2024-09-17', 2200.00),
(8, 8, '2024-09-18', 3500.00),
(9, 9, '2024-09-19', 2800.00),
(10, 10, '2024-09-20', 3000.00);





--4.1Write query to retrieve all customer information:
  select * from Customers;




--4.2: Query accounts for a specific customer:
 	select * from accounts where customer_id=5;
	 

--4.3: Find the customer name and account balance for each account
select concat(first_name,' ',last_name) as customer_name,a.account_type, balance
 from customers c join accounts a on c.customer_id=a.customer_id ;
 

 --4.4: Analyze customer loan balances:
	select concat(first_name,' ',last_name) as customer_name,l.loan_amount
 from customers c join loans l on c.customer_id=l.customer_id ;

 



 --4.5: List all customers who have made a transaction in the 2024-03
select distinct concat(first_name, last_name) as customername , transaction_date 
from customers c 
join accounts a on   c.customer_id=a.customer_id 
join transactions t on a.account_id=t.account_id 
    where t.transaction_date between '2024-03-01' and '2024-03-31';


 



--Step 5:  Aggregation and Insights

--5.1: Calculate the total balance across all accounts for each customer:

SELECT concat(first_name, last_name) as customername , SUM(a.balance) AS total_balance
FROM customers c
JOIN accounts a ON c.customer_id = a.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name;

 



 --5.2: Calculate the average loan amount for each loan term:

SELECT loan_term, AVG(loan_amount) AS average_loan_amount
FROM loans
GROUP BY loan_term;

 


--5.3: Find the total loan amount and interest across all loans:

 SELECT SUM(loan_amount) AS total_loan_amount, SUM(loan_amount * (interest_rate / 100)) AS total_interest FROM loans;

 




 --5.4: Find the most frequent transaction type


SELECT transaction_type, COUNT(*) AS count
FROM transactions
GROUP BY transaction_type
ORDER BY count DESC;  

 

--5.5: Analyze transactions by account and transaction type:

SELECT account_id, transaction_type, COUNT(*) AS transaction_count, SUM(transaction_amount) AS total_amount
FROM transactions
GROUP BY account_id, transaction_type;


 


--Step 6: Advanced Analysis

 --6.1: Create a view of active loans with payments greater than $1000:

CREATE VIEW active_loans_with_high_payments AS
SELECT l.loan_id, c.first_name, c.last_name, l.loan_amount, lp.payment_amount
FROM loans l
JOIN customers c ON l.customer_id = c.customer_id
JOIN loan_payments lp ON l.loan_id = lp.loan_id
WHERE lp.payment_amount > 1000;


 


   --6.2: Create an index on `transaction_date` in the `transactions` table for performance optimization:

CREATE INDEX idx_transaction_date ON transactions(transaction_date);

