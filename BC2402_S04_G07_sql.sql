-- BC2402_S04_G07_sql codes

-- 1. How many categories are in [customer_suppport]? (TIP: You need to decide whether to clean up the data.)
SELECT DISTINCT(category)
FROM customer_support
ORDER BY category COLLATE utf8mb4_bin DESC; -- 36 rows returned
-- It seems like only the capitalised entries in the `category` column clearly represent defined categories of customer support queries, while the remaining 28 rows contain random texts and results that do not fit as customer support queries. 
-- Hence, since a substantial amount of irrelevant data (28 rows) can undermine the integrity of our dataset, we will be cleaning the data by removing these rows for a more accurate analysis.
SET SQL_SAFE_UPDATES = 0;
DELETE FROM customer_support
WHERE category NOT IN ('SHIPPING', 'REFUND', 'PAYMENT', 'ORDER', 'INVOICE', 'FEEDBACK', 'CONTACT', 'CANCEL');
SELECT COUNT(DISTINCT category) AS NumberOfCategories
FROM customer_support;
SET SQL_SAFE_UPDATES = 1; -- Therefore, after cleaning the data, there are 8 categories in customer_support, namely 'SHIPPING', 'REFUND', 'PAYMENT', 'ORDER', 'INVOICE', 'FEEDBACK', 'CONTACT', 'CANCEL'.

-- 2. [customer_suppport] For each category, display the number of records that contained colloquial variation and offensive language. (TIP: Refer to language generation tags.)
SELECT category,
COUNT(CASE WHEN flags LIKE '%Q%' THEN 1 END) AS numOfColloquialVariation,
COUNT(CASE WHEN flags LIKE '%W%' THEN 1 END) AS numOfOffensiveLanguage
FROM customer_support
GROUP BY category
ORDER BY category;

-- 3. [flight_delay] For each airline, display the instances of cancellations and delays. (Hint: UNION, $merge)
SELECT airline, 
COUNT(*) AS numOfFlights, 'Cancelled' AS Status
FROM flight_delay
WHERE Cancelled = 1
GROUP BY airline

UNION

SELECT airline, 
COUNT(*) AS numOfFlights, 'Delayed' AS Status
FROM flight_delay
WHERE ArrDelay > 0 
GROUP BY airline
ORDER BY airline, Status; -- Notably, there are no flight cancellations

-- 4. [flight_delay] For each month, which route has the most instances of delays? (TIP: What are the first and last dates in the data?)
SELECT MonthName, Route, DelayInstances
FROM (
    SELECT 
        MONTHNAME(STR_TO_DATE(Date, '%d-%m-%Y')) AS MonthName,
        CONCAT(Origin, ' - ', Dest) AS Route,
        COUNT(*) AS DelayInstances,
        RANK() OVER(PARTITION BY MONTH(STR_TO_DATE(Date, '%d-%m-%Y')) ORDER BY COUNT(*) DESC) AS RouteRank
    FROM flight_delay
    WHERE ArrDelay > 0
    AND STR_TO_DATE(Date, '%d-%m-%Y') BETWEEN '2019-01-01' AND '2019-12-06'
    GROUP BY MONTH(STR_TO_DATE(Date, '%d-%m-%Y')), MonthName, Route
) AS MonthlyRoutes
WHERE RouteRank = 1
ORDER BY FIELD(MonthName, 'January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December');

-- 5. [sia_stock] For the year 2023, display the quarter-on-quarter changes in high and low prices and the quarterly average price.
WITH QuarterlyData AS (
    SELECT         
        YEAR(STR_TO_DATE(StockDate, '%m/%d/%Y')) AS Year,
        QUARTER(STR_TO_DATE(StockDate, '%m/%d/%Y')) AS Quarter,
        MAX(High) AS QuarterlyHigh,
        MIN(Low) AS QuarterlyLow,
        ROUND(AVG(Price), 2) AS QuarterlyAvgPrice,
        
        -- QoQ change for high price
        ROUND((MAX(High) - LAG(MAX(High)) OVER (ORDER BY YEAR(STR_TO_DATE(StockDate, '%m/%d/%Y')), QUARTER(STR_TO_DATE(StockDate, '%m/%d/%Y')))) 
            / LAG(MAX(High)) OVER (ORDER BY YEAR(STR_TO_DATE(StockDate, '%m/%d/%Y')), QUARTER(STR_TO_DATE(StockDate, '%m/%d/%Y'))) * 100, 2) AS QoQ_High_Change,
        
        -- QoQ change for low price
        ROUND((MIN(Low) - LAG(MIN(Low)) OVER (ORDER BY YEAR(STR_TO_DATE(StockDate, '%m/%d/%Y')), QUARTER(STR_TO_DATE(StockDate, '%m/%d/%Y')))) 
            / LAG(MIN(Low)) OVER (ORDER BY YEAR(STR_TO_DATE(StockDate, '%m/%d/%Y')), QUARTER(STR_TO_DATE(StockDate, '%m/%d/%Y'))) * 100, 2) AS QoQ_Low_Change
    FROM sia_stock
    WHERE YEAR(STR_TO_DATE(StockDate, '%m/%d/%Y')) IN (2022, 2023)
    GROUP BY Year, Quarter
)

SELECT 
	Year,
    Quarter,
    QuarterlyHigh,
    QuarterlyLow,
    QuarterlyAvgPrice,
    QoQ_High_Change,
    QoQ_Low_Change
FROM QuarterlyData
WHERE Year = 2023
ORDER BY Quarter;

-- 6.	[customer_booking] For each sales_channel and each route, display the following ratios
-- average length_of_stay / average flight_hour 
-- average wants_extra_baggage / average flight_hour
-- average wants_preferred_seat / average flight_hour
-- average wants_in_flight_meals / average flight_hour
-- Our underlying objective: Are there any correlations between flight hours, length of stay, and various preferences (i.e., extra baggage, preferred seats, in-flight meals)?
SELECT 
    sales_channel,
    route,
    flight_duration,
    ROUND(AVG(length_of_stay) / AVG(flight_duration), 2) AS AverageLengthOfStayPerFlightDuration,
    ROUND(AVG(wants_extra_baggage) / AVG(flight_duration), 2) AS AverageBaggageRequestPerFlightDuration,
    ROUND(AVG(wants_preferred_seat) / AVG(flight_duration), 2) AS AveragePreferredSeatRequestPerFlightDuration,
    ROUND(AVG(wants_in_flight_meals) / AVG(flight_duration), 2) AS AverageInFlightMealRequestPerFlightDuration
FROM customer_booking
GROUP BY route, sales_channel, flight_duration
ORDER BY route, sales_channel, flight_duration;

-- `AverageLengthOfStayPerFlightHour` (in days)
-- Significance: 1. A higher ratio suggests that passengers are staying longer at the destination relative to the flight duration --> passengers might be tourists or leisure travellers.
--               2. A lower ratio implies shorter stays relative to the flight duration --> passengers may be on business or quick trips.

-- `AverageBaggageRequestPerFlightHour` (requested = 1; not requested = 0)
-- Significance: Represents the proportion of passengers requesting extra baggage. A higher ratio indicates that for each hour of flight, a larger proportion of passengers are requesting extra baggage (vice versa).

-- `AveragePreferredSeatRequestPerFlightHour`
-- Significance: A higher ratio indicates that for each hour of flight, a larger proportion of passengers are requesting preferred seating (vice versa).

-- `AverageInFlightMealRequestPerFlightHour`
-- Significance: A higher ratio indicates that for each hour of flight, a larger proportion of passengers are requesting in-flight meals (vice versa).

-- 7. [airlines_reviews] Airline seasonality. For each Airline and Class, display the averages of SeatComfort, FoodnBeverages, InflightEntertainment, ValueForMoney, and OverallRating for the seasonal and non-seasonal periods, respectively.
-- Since seasonality refers to fluctuations in flight demand due to external factors, we will assume that the seasonality of flights is determined by the variable `MonthFlown`.
SELECT 
    Airline,
    Class,
    CASE 
        WHEN SUBSTRING(MonthFlown, 1, 3) IN ('Jun', 'Jul', 'Aug', 'Sep') THEN 'Seasonal'
        ELSE 'Non-Seasonal'
    END AS Period,
    ROUND(AVG(SeatComfort), 2) AS AvgSeatComfort,
    ROUND(AVG(FoodnBeverages), 2) AS AvgFoodnBeverages,
    ROUND(AVG(InflightEntertainment), 2) AS AvgInflightEntertainment,
    ROUND(AVG(ValueForMoney), 2) AS AvgValueForMoney,
    ROUND(AVG(OverallRating), 2) AS AvgOverallRating
FROM airlines_reviews
GROUP BY 
    Airline, 
    Class, 
    Period
ORDER BY 
    Airline, 
    FIELD(Class, 'First Class', 'Business Class', 'Premium Economy', 'Economy Class'),
    Period;

-- 8. [airlines_reviews] What are the common complaints? For each Airline and TypeofTraveller, list the top 5 common issues.
-- (a) Count the frequency of words to shortlist categories of complaints
WITH Words AS (
  SELECT LOWER(word) AS word
  FROM (
    SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(Reviews, ' ', numbers.n), ' ', -1) AS word
    FROM airlines_reviews
    JOIN (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10) numbers
	ON CHAR_LENGTH(Reviews) - CHAR_LENGTH(REPLACE(Reviews, ' ', '')) >= numbers.n - 1
    WHERE Recommended = 'no'
    UNION ALL
    SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(Reviews, ' ', numbers.n), ' ', -1) AS word
    FROM airlines_reviews
    JOIN (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10) numbers
    ON CHAR_LENGTH(Reviews) - CHAR_LENGTH(REPLACE(Reviews, ' ', '')) >= numbers.n - 1
    WHERE Recommended = 'no'
  ) AS all_words
),
WordCounts AS (
  SELECT word, COUNT(*) AS count
  FROM Words
  GROUP BY word
)
SELECT word, count
FROM WordCounts
ORDER BY count DESC;

-- (b) We manually identified some words indicative of complaint categories, such as "service", "experience", "food", "delayed", "seat(s)", "refund", "baggage", "suitcase", "cancelled" etc. Hence, we grouped them into different complaint categories.
-- Below is the general overview of the distribution of complaints.
 SELECT 
    Airline,
    TypeofTraveller,
    
    -- Service Quality and Customer Interaction
    COUNT(CASE WHEN Reviews LIKE '%service%' OR Reviews LIKE '%experience%' OR Reviews LIKE '%staff%' OR Reviews LIKE '%communication%' OR Reviews LIKE '%rude%' OR Reviews LIKE '%airline%' THEN 1 END) AS ServiceQualityIssues,
    
    -- Timeliness and Operational Issues
    COUNT(CASE WHEN Reviews LIKE '%delayed%' OR Reviews LIKE '%cancelled%' OR Reviews LIKE '%ticket%' OR Reviews LIKE '%check-in%' OR Reviews LIKE '%boarding%' THEN 1 END) AS TimelinessAndOperationalIssues,
    
    -- Seating and Comfort
    COUNT(CASE WHEN Reviews LIKE '%seat%' OR Reviews LIKE '%uncomfortable%' OR Reviews LIKE '%space%' THEN 1 END) AS SeatingAndComfortIssues,
    
    -- Baggage Handling
    COUNT(CASE WHEN Reviews LIKE '%suitcase%' OR Reviews LIKE '%baggage%' OR Reviews LIKE '%lost%' THEN 1 END) AS BaggageHandlingIssues,
    
    -- Food and In-flight Amenities
    COUNT(CASE WHEN Reviews LIKE '%food%' OR Reviews LIKE '%meal%' THEN 1 END) AS FoodAndInflightAmenitiesIssues,
    
    -- Refunds and Compensation
    COUNT(CASE WHEN Reviews LIKE '%refund%' OR Reviews LIKE '%compensation%' THEN 1 END) AS RefundsAndCompensationIssues,
    
    -- Booking and Ticketing Issues
    COUNT(CASE WHEN Reviews LIKE '%ticket%' OR Reviews LIKE '%booking%' OR Reviews LIKE '%check%' OR Reviews LIKE '%check-in%' THEN 1 END) AS BookingAndTicketingIssues,
    
    -- Flight Experience (General)
    COUNT(CASE WHEN Reviews LIKE '%airways%' OR Reviews LIKE '%flight%' OR Reviews LIKE '%boarding%' OR Reviews LIKE '%communication%' THEN 1 END) AS FlightExperienceIssues,
    
    -- Cabin Cleanliness and Condition
    COUNT(CASE WHEN Reviews LIKE '%cleanliness%' OR Reviews LIKE '%space%' OR Reviews LIKE '%comfort%' THEN 1 END) AS CabinCleanlinessIssues
    
FROM airlines_reviews
WHERE Recommended = 'no' -- Focus on negative reviews only
GROUP BY Airline, TypeofTraveller
ORDER BY Airline, TypeofTraveller;

-- (c) Below is the ranking of the top 5 common issues prevalent among the different types of travellers in different airlines.
WITH ComplaintCounts AS (
    SELECT 
        Airline,
        TypeofTraveller,
        
        -- Service Quality and Customer Interaction
        COUNT(CASE WHEN Reviews LIKE '%service%' OR Reviews LIKE '%experience%' OR Reviews LIKE '%staff%' OR Reviews LIKE '%communication%' OR Reviews LIKE '%rude%' OR Reviews LIKE '%airline%' THEN 1 END) AS ServiceQualityIssues,
        
        -- Timeliness and Operational Issues
        COUNT(CASE WHEN Reviews LIKE '%delayed%' OR Reviews LIKE '%cancelled%' OR Reviews LIKE '%ticket%' OR Reviews LIKE '%check-in%' OR Reviews LIKE '%boarding%' THEN 1 END) AS TimelinessAndOperationalIssues,
        
        -- Seating and Comfort
        COUNT(CASE WHEN Reviews LIKE '%seat%' OR Reviews LIKE '%uncomfortable%' OR Reviews LIKE '%space%' THEN 1 END) AS SeatingAndComfortIssues,
        
        -- Baggage Handling
        COUNT(CASE WHEN Reviews LIKE '%suitcase%' OR Reviews LIKE '%baggage%' OR Reviews LIKE '%lost%' THEN 1 END) AS BaggageHandlingIssues,
        
        -- Food and In-flight Amenities
        COUNT(CASE WHEN Reviews LIKE '%food%' OR Reviews LIKE '%meal%' THEN 1 END) AS FoodAndInflightAmenitiesIssues,
        
        -- Refunds and Compensation
        COUNT(CASE WHEN Reviews LIKE '%refund%' OR Reviews LIKE '%compensation%' THEN 1 END) AS RefundsAndCompensationIssues,
        
        -- Booking and Ticketing Issues
        COUNT(CASE WHEN Reviews LIKE '%ticket%' OR Reviews LIKE '%booking%' OR Reviews LIKE '%check%' OR Reviews LIKE '%check-in%' THEN 1 END) AS BookingAndTicketingIssues,
        
        -- Flight Experience (General)
        COUNT(CASE WHEN Reviews LIKE '%airways%' OR Reviews LIKE '%flight%' OR Reviews LIKE '%boarding%' OR Reviews LIKE '%communication%' THEN 1 END) AS FlightExperienceIssues,
        
        -- Cabin Cleanliness and Condition
        COUNT(CASE WHEN Reviews LIKE '%cleanliness%' OR Reviews LIKE '%space%' OR Reviews LIKE '%comfort%' THEN 1 END) AS CabinCleanlinessIssues
    FROM airlines_reviews
    WHERE Recommended = 'no'  -- Assume that passengers who did not recommend the flight to others are dissatisfied with their experience and are likely to have more complaints.
    GROUP BY Airline, TypeofTraveller
),
RankedComplaints AS (
    SELECT
        Airline,
        TypeofTraveller,
        'ServiceQualityIssues' AS ComplaintCategory,
        ServiceQualityIssues AS ComplaintCount
    FROM ComplaintCounts
    UNION ALL
    SELECT
        Airline,
        TypeofTraveller,
        'TimelinessAndOperationalIssues' AS ComplaintCategory,
        TimelinessAndOperationalIssues AS ComplaintCount
    FROM ComplaintCounts
    UNION ALL
    SELECT
        Airline,
        TypeofTraveller,
        'SeatingAndComfortIssues' AS ComplaintCategory,
        SeatingAndComfortIssues AS ComplaintCount
    FROM ComplaintCounts
    UNION ALL
    SELECT
        Airline,
        TypeofTraveller,
        'BaggageHandlingIssues' AS ComplaintCategory,
        BaggageHandlingIssues AS ComplaintCount
    FROM ComplaintCounts
    UNION ALL
    SELECT
        Airline,
        TypeofTraveller,
        'FoodAndInflightAmenitiesIssues' AS ComplaintCategory,
        FoodAndInflightAmenitiesIssues AS ComplaintCount
    FROM ComplaintCounts
    UNION ALL
    SELECT
        Airline,
        TypeofTraveller,
        'RefundsAndCompensationIssues' AS ComplaintCategory,
        RefundsAndCompensationIssues AS ComplaintCount
    FROM ComplaintCounts
    UNION ALL
    SELECT
        Airline,
        TypeofTraveller,
        'BookingAndTicketingIssues' AS ComplaintCategory,
        BookingAndTicketingIssues AS ComplaintCount
    FROM ComplaintCounts
    UNION ALL
    SELECT
        Airline,
        TypeofTraveller,
        'FlightExperienceIssues' AS ComplaintCategory,
        FlightExperienceIssues AS ComplaintCount
    FROM ComplaintCounts
    UNION ALL
    SELECT
        Airline,
        TypeofTraveller,
        'CabinCleanlinessIssues' AS ComplaintCategory,
        CabinCleanlinessIssues AS ComplaintCount
    FROM ComplaintCounts
),
TopComplaints AS (
    SELECT 
        Airline, 
        TypeofTraveller, 
        ComplaintCategory, 
        ComplaintCount,
        ROW_NUMBER() OVER (PARTITION BY Airline, TypeofTraveller ORDER BY ComplaintCount DESC) AS ComplaintRank
    FROM RankedComplaints
)
SELECT 
    Airline,
    TypeofTraveller,
    ComplaintCategory,
    ComplaintCount
FROM TopComplaints
WHERE ComplaintRank <= 5  -- Show only the top 5 complaints for each airline and traveler type
ORDER BY Airline, TypeofTraveller, ComplaintRank;

-- 9. [airlines_reviews] and additional data*. Are there any systematic differences in customer preferences/complaints pre- and post- COVID specific to Singapore Airlines? 
-- In addition to customer satisfaction, what do you think contributed to the strong performance of Singapore Airlines in recent periods?

-- Pre-COVID: Period before the outbreak of COVID-19 (before 23 Jan 2020)
-- Post-COVID: Period after the initial impact of the pandemic began to subside (after 13 Feb 2023 where border restrictions are lifted and normal travel resumes).

-- Pre-COVID vs Post-COVID ranking of complaints (only post-COVID results printed, needs correction) 
WITH ComplaintCounts AS (
    SELECT 
        CASE
            WHEN STR_TO_DATE(CONCAT('01-', MonthFlown), '%d-%b-%y') < STR_TO_DATE('2020-01-23', '%Y-%m-%d') THEN 'Pre-COVID'
            WHEN STR_TO_DATE(CONCAT('01-', MonthFlown), '%d-%b-%y') > STR_TO_DATE('2023-02-13', '%Y-%m-%d') THEN 'Post-COVID'
        END AS COVID_Period,
        
        -- Service Quality and Customer Interaction
        COUNT(CASE WHEN Reviews LIKE '%service%' OR Reviews LIKE '%experience%' OR Reviews LIKE '%staff%' OR Reviews LIKE '%communication%' OR Reviews LIKE '%rude%' OR Reviews LIKE '%airline%' THEN 1 END) AS ServiceQualityIssues,
        
        -- Timeliness and Operational Issues
        COUNT(CASE WHEN Reviews LIKE '%delayed%' OR Reviews LIKE '%cancelled%' OR Reviews LIKE '%ticket%' OR Reviews LIKE '%check-in%' OR Reviews LIKE '%boarding%' THEN 1 END) AS TimelinessAndOperationalIssues,
        
        -- Seating and Comfort
        COUNT(CASE WHEN Reviews LIKE '%seat%' OR Reviews LIKE '%uncomfortable%' OR Reviews LIKE '%space%' THEN 1 END) AS SeatingAndComfortIssues,
        
        -- Baggage Handling
        COUNT(CASE WHEN Reviews LIKE '%suitcase%' OR Reviews LIKE '%baggage%' OR Reviews LIKE '%lost%' THEN 1 END) AS BaggageHandlingIssues,
        
        -- Food and In-flight Amenities
        COUNT(CASE WHEN Reviews LIKE '%food%' OR Reviews LIKE '%meal%' THEN 1 END) AS FoodAndInflightAmenitiesIssues,
        
        -- Refunds and Compensation
        COUNT(CASE WHEN Reviews LIKE '%refund%' OR Reviews LIKE '%compensation%' THEN 1 END) AS RefundsAndCompensationIssues,
        
        -- Booking and Ticketing Issues
        COUNT(CASE WHEN Reviews LIKE '%ticket%' OR Reviews LIKE '%booking%' OR Reviews LIKE '%check%' OR Reviews LIKE '%check-in%' THEN 1 END) AS BookingAndTicketingIssues,
        
        -- Flight Experience (General)
        COUNT(CASE WHEN Reviews LIKE '%airways%' OR Reviews LIKE '%flight%' OR Reviews LIKE '%boarding%' OR Reviews LIKE '%communication%' THEN 1 END) AS FlightExperienceIssues,
        
        -- Cabin Cleanliness and Condition
        COUNT(CASE WHEN Reviews LIKE '%cleanliness%' OR Reviews LIKE '%space%' OR Reviews LIKE '%comfort%' THEN 1 END) AS CabinCleanlinessIssues
    FROM airlines_reviews
    WHERE Recommended = 'no' AND Airline = 'Singapore Airlines'
    GROUP BY COVID_Period
    HAVING COVID_Period IS NOT NULL  -- Exclude any NULL values
),
RankedComplaints AS (
    SELECT
        COVID_Period,
        'ServiceQualityIssues' AS ComplaintCategory,
        ServiceQualityIssues AS ComplaintCount
    FROM ComplaintCounts
    UNION ALL
    SELECT
        COVID_Period,
        'TimelinessAndOperationalIssues' AS ComplaintCategory,
        TimelinessAndOperationalIssues AS ComplaintCount
    FROM ComplaintCounts
    UNION ALL
    SELECT
        COVID_Period,
        'SeatingAndComfortIssues' AS ComplaintCategory,
        SeatingAndComfortIssues AS ComplaintCount
    FROM ComplaintCounts
    UNION ALL
    SELECT
        COVID_Period,
        'BaggageHandlingIssues' AS ComplaintCategory,
        BaggageHandlingIssues AS ComplaintCount
    FROM ComplaintCounts
    UNION ALL
    SELECT
        COVID_Period,
        'FoodAndInflightAmenitiesIssues' AS ComplaintCategory,
        FoodAndInflightAmenitiesIssues AS ComplaintCount
    FROM ComplaintCounts
    UNION ALL
    SELECT
        COVID_Period,
        'RefundsAndCompensationIssues' AS ComplaintCategory,
        RefundsAndCompensationIssues AS ComplaintCount
    FROM ComplaintCounts
    UNION ALL
    SELECT
        COVID_Period,
        'BookingAndTicketingIssues' AS ComplaintCategory,
        BookingAndTicketingIssues AS ComplaintCount
    FROM ComplaintCounts
    UNION ALL
    SELECT
        COVID_Period,
        'FlightExperienceIssues' AS ComplaintCategory,
        FlightExperienceIssues AS ComplaintCount
    FROM ComplaintCounts
    UNION ALL
    SELECT
        COVID_Period,
        'CabinCleanlinessIssues' AS ComplaintCategory,
        CabinCleanlinessIssues AS ComplaintCount
    FROM ComplaintCounts
),
TopComplaints AS (
    SELECT 
        COVID_Period, 
        ComplaintCategory, 
        ComplaintCount,
        ROW_NUMBER() OVER (PARTITION BY COVID_Period ORDER BY ComplaintCount DESC) AS ComplaintRank
    FROM RankedComplaints
)
SELECT 
    COVID_Period,
    ComplaintCategory,
    ComplaintCount
FROM TopComplaints
WHERE ComplaintRank <= 5  -- Show only the top 5 complaints for each COVID period
ORDER BY COVID_Period, ComplaintRank;

-- Pre-COVID vs Post-COVID ratings: post < pre (needs correction)
WITH PrePostCovid AS (
    SELECT 
        CASE
            WHEN STR_TO_DATE(CONCAT('01-', MonthFlown), '%d-%b-%y') < STR_TO_DATE('2020-01-23', '%Y-%m-%d') THEN 'Pre-COVID'
            WHEN STR_TO_DATE(CONCAT('01-', MonthFlown), '%d-%b-%y') > STR_TO_DATE('2023-02-13', '%Y-%m-%d') THEN 'Post-COVID'
            ELSE 'During-COVID'
        END AS COVID_Period,
        AVG(SeatComfort) AS AvgSeatComfort,
        AVG(FoodnBeverages) AS AvgFoodnBeverages,
        AVG(InflightEntertainment) AS AvgInflightEntertainment,
        AVG(ValueForMoney) AS AvgValueForMoney,
        AVG(OverallRating) AS AvgOverallRating
    FROM airlines_reviews
    WHERE 
        (STR_TO_DATE(CONCAT('01-', MonthFlown), '%d-%b-%y') < STR_TO_DATE('2020-01-23', '%Y-%m-%d')
        OR STR_TO_DATE(CONCAT('01-', MonthFlown), '%d-%b-%y') > STR_TO_DATE('2023-02-13', '%Y-%m-%d'))
        AND Airline = 'Singapore Airlines'
    GROUP BY COVID_Period
)
SELECT 
    COVID_Period,
    ROUND(AvgSeatComfort, 2) AS AvgSeatComfort,
    ROUND(AvgFoodnBeverages, 2) AS AvgFoodnBeverages,
    ROUND(AvgInflightEntertainment, 2) AS AvgInflightEntertainment,
    ROUND(AvgValueForMoney, 2) AS AvgValueForMoney,
    ROUND(AvgOverallRating, 2) AS AvgOverallRating
FROM PrePostCovid
ORDER BY COVID_Period;
