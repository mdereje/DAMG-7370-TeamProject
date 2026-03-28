
INSERT INTO chinook.Customer (
    FirstName, LastName, Company, Address, City, State, Country,
    PostalCode, Phone, Fax, Email, SupportRepId
)
VALUES
('John', 'Doe', 'ABC Corp', '123 Main St', 'New York', 'NY', 'USA', '10001', '1234567890', NULL, 'john.doe@email.com', 3),
('Jane', 'Smith', 'XYZ Inc', '456 Park Ave', 'Chicago', 'IL', 'USA', '60601', '2345678901', NULL, 'jane.smith@email.com', 3),
('Mike', 'Johnson', NULL, '789 Elm St', 'San Francisco', 'CA', 'USA', '94101', '3456789012', NULL, 'mike.johnson@email.com', 4),
('Emily', 'Davis', 'TechSoft', '321 Oak St', 'Seattle', 'WA', 'USA', '98101', '4567890123', NULL, 'emily.davis@email.com', 4),
('Chris', 'Brown', NULL, '654 Pine St', 'Boston', 'MA', 'USA', '02101', '5678901234', NULL, 'chris.brown@email.com', 5);


INSERT INTO chinook.Invoice (
    CustomerId, InvoiceDate, BillingAddress, BillingCity,
    BillingState, BillingCountry, BillingPostalCode, Total
)
VALUES
(65, GETDATE(), '123 Main St', 'New York', 'NY', 'USA', '10001', 25.99),
(66, GETDATE(), '456 Park Ave', 'Chicago', 'IL', 'USA', '60601', 15.49),
(67, GETDATE(), '789 Elm St', 'San Francisco', 'CA', 'USA', '94101', 40.00),
(68, GETDATE(), '321 Oak St', 'Seattle', 'WA', 'USA', '98101', 60.75),
(69, GETDATE(), '654 Pine St', 'Boston', 'MA', 'USA', '02101', 10.99);


INSERT INTO chinook.InvoiceLine (InvoiceId, TrackId, UnitPrice, Quantity)
VALUES
    (413, 1, 0.99, 1),
    (414, 2, 1.99, 2),
    (415, 3, 0.99, 1),
    (416, 4, 1.99, 3),
    (417, 5, 0.99, 1);


INSERT INTO chinook.Artist (Name)
VALUES
('The Midnight Echo'),
('Neon Waves'),
('Crimson Pulse'),
('Golden Horizon'),
('Silver Strings');



select * from chinook.customer;

select * from chinook.invoice;

select * from chinook.invoiceline;

select * from chinook.artist;

select * from chinook.track;



UPDATE chinook.Customer
SET City = 'Los Angeles',
    State = 'CA'
WHERE CustomerId = 68;

UPDATE chinook.Customer
SET Company = 'Updated Corp',
    Phone = '1234567890'
WHERE CustomerId = 69;

UPDATE chinook.Customer
SET Email = 'updated.mike@email.com',
    Country = 'Canada'
WHERE CustomerId = 67;




UPDATE chinook.Invoice
SET BillingCity = 'Los Angeles',
    BillingState = 'CA',
    Total = 30.50
WHERE InvoiceId = 413;

UPDATE chinook.Invoice
SET BillingCountry = 'Canada',
    Total = 20.00
WHERE InvoiceId = 414;

UPDATE chinook.Invoice
SET BillingAddress = '999 Updated St',
    BillingPostalCode = '99999'
WHERE InvoiceId = 415;




UPDATE chinook.Artist
SET Name = 'Midnight Echoes'
WHERE ArtistId = 276;

UPDATE chinook.Artist
SET Name = 'Neon Wave Collective'
WHERE ArtistId = 277;

UPDATE chinook.Artist
SET Name = 'Crimson Pulse Band'
WHERE ArtistId = 278;




UPDATE chinook.InvoiceLine
SET 
    UnitPrice = 2.99,
    Quantity  = 2
WHERE InvoiceLineId IN (2246, 2247, 2248);