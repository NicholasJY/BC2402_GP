//BC2402_GP_S04_G07_NoSQL

// Question 1
db.customer_support.deleteMany({
    category: { $nin: ['ORDER', 'SHIPPING', 'CANCEL', 'INVOICE', 'PAYMENT', 'REFUND', 'FEEDBACK', 'CONTACT'] }
});

// Question 2
db.customer_support.aggregate([
    {
        $project: {
            category: 1,
            ColloquialFlag: { $cond: [{ $regexMatch: { input: "$flags", regex: "Q" } }, 1, 0] },
            OffensiveFlag: { $cond: [{ $regexMatch: { input: "$flags", regex: "W" } }, 1, 0] }
        } // Create ColloquialFlag and OffensiveFlag, Check if data contains 'Q' or 'W' respectively
    },
    {
        $group: {
            _id: "$category",
            ColloquialCount: { $sum: "$ColloquialFlag" },
            OffensiveCount: { $sum: "$OffensiveFlag" }
        } // Group documents by category and calculate the total count
    },
    {
        $sort: { _id: 1 } // Sorts the grouped results by category in asc order
    },
    {
        $project: {
            Category: "$_id",
             _id: 0,
            ColloquialCount: 1,
            OffensiveCount: 1
        }
    }
]);

//Question 3
db.flight_delay.aggregate([
  {$match: { Cancelled: 1 }},
  {$group: {_id: "$Airline",numOfFlights: { $sum: 1 }}},
  {$addFields: { Status: "Cancelled" }},
  {$unionWith: {
      coll: "flight_delay",pipeline: [
        {$match: { ArrDelay: { $gt: 0 } }},
        {$group: {_id: "$Airline",numOfFlights: { $sum: 1 }}},
        {$addFields: { Status: "Delayed" }}]}},
  {$sort: { _id: 1, Status: 1 }}]);
  
  //Question 4 (?)
  
 
 //Question 5
db.sia_stock.aggregate([
    {$match: {"StockDate": {$regex: /2023/}}},
    {$project: {quarter: {$concat: ["Q",
                    {$toString: {$ceil: {$divide: [{$month: {$dateFromString: {
                        dateString: "$StockDate", format: "%m/%d/%Y"}}}, 3]}}}]},
            high: "$High", low: "$Low", price: "$Price"}},
    {$group: {
            _id: "$quarter",
            high_price: {$max: "$high"},
            low_price: {$min: "$low"},
            average_price: {$avg: "$price"}
        }},
    {$sort: {_id: 1}},
    {$group: {
            _id: null,
            quarters: {$push: { 
                quarter: "$_id", 
                high_price: "$high_price", 
                low_price: "$low_price", 
                average_price: "$average_price" 
            }}}},
    {$unwind: "$quarters"},
    {// Add fields for quarter-on-quarter changes
        $group: {_id: null,
            all_quarters: {$push: "$quarters"}}},
    {// Calculate changes for high, low, and average prices
        $project: {_id:0, 
            quarters: {
                $map: {
                    input: {$range: [0, {$size: "$all_quarters"}]},
                    as: "index",
                    in: {
                        quarter: {$arrayElemAt: ["$all_quarters", "$$index"]},
                        high_change: {
                            $subtract: [
                                {$arrayElemAt: ["$all_quarters.high_price", "$$index"]},
                                {$cond: {if: {$gt: ["$$index", 0]}, then: {$arrayElemAt: ["$all_quarters.high_price", {$subtract: ["$$index", 1]}]}, else: 0}}
                            ]
                        },
                        low_change: {
                            $subtract: [
                                {$arrayElemAt: ["$all_quarters.low_price", "$$index"]},
                                {$cond: {if: {$gt: ["$$index", 0]}, then: {$arrayElemAt: ["$all_quarters.low_price", {$subtract: ["$$index", 1]}]}, else: 0}}
                            ]
                        },
                        average_change: {
                            $subtract: [
                                {$arrayElemAt: ["$all_quarters.average_price", "$$index"]},
                                {$cond: {if: {$gt: ["$$index", 0] }, then: {$arrayElemAt: ["$all_quarters.average_price", {$subtract: ["$$index", 1]}]}, else: 0}}
                            ]
                        }
                    }
                }
            }
        }
    }
])
  


// Question 6
db.customer_booking.aggregate([
    {
        $group: { // Groups documents by sales_channel and route
            _id: { sales_channel: "$sales_channel", route: "$route" },
            avg_length_of_stay: { $avg: "$length_of_stay" },
            avg_flight_hour: { $avg: "$flight_hour" },
            avg_wants_extra_baggage: { $avg: "$wants_extra_baggage" },
            avg_wants_preferred_seat: { $avg: "$wants_preferred_seat" },
            avg_wants_in_flight_meals: { $avg: "$wants_in_flight_meals" }
        } //Calculate average
    },
    {$project: { // Calculate ratio
            sales_channel: "$_id.sales_channel",
            route: "$_id.route",
            avg_length_of_stay_per_flight_hour: { 
                $cond: { 
                    if: { $eq: ["$avg_flight_hour", 0] }, 
                    then: null, 
                    else: { $divide: ["$avg_length_of_stay", "$avg_flight_hour"] } 
                }
            },
            avg_wants_extra_baggage_per_flight_hour: { 
                $cond: { 
                    if: { $eq: ["$avg_flight_hour", 0] }, 
                    then: null, 
                    else: { $divide: ["$avg_wants_extra_baggage", "$avg_flight_hour"] } 
                }
            },
            avg_wants_preferred_seat_per_flight_hour: { 
                $cond: { 
                    if: { $eq: ["$avg_flight_hour", 0] }, 
                    then: null, 
                    else: { $divide: ["$avg_wants_preferred_seat", "$avg_flight_hour"] } 
                }
            },
            avg_wants_in_flight_meals_per_flight_hour: { 
                $cond: { 
                    if: { $eq: ["$avg_flight_hour", 0] }, 
                    then: null, 
                    else: { $divide: ["$avg_wants_in_flight_meals", "$avg_flight_hour"] } 
                }
            }
        }
    },
    {
        $sort: { "sales_channel": 1, "route": 1 } // Sort the results by sales_channel and route in ascending order
    }
]);



//Question 7
db.airline_reviews.aggregate([
  {
    $addFields: {
      Seasonality: {
        $cond: [
          { $in: [{ $substr: ["$MonthFlown", 0, 3] }, ["Jun", "Jul", "Aug", "Sep"]] },
          "Seasonal",
          "Non-Seasonal"
        ]
      }
    }
  },
  {$group: {
      _id: {
        airline: "$Airline",
        class: "$Class",
        seasonality: "$Seasonality"
      },
      AvgSeatComfort: { $avg: "$SeatComfort" },
      AvgFoodnBeverages: { $avg: "$FoodnBeverages" },
      AvgInflightEntertainment: { $avg: "$InflightEntertainment" },
      AvgValueForMoney: { $avg: "$ValueForMoney" },
      AvgOverallRating: { $avg: "$OverallRating" }}
  },
  {$project: {
      _id: 0,
      Airline: "$_id.airline",
      Class: "$_id.class",
      Seasonality: "$_id.seasonality",
      AvgSeatComfort: 1,
      AvgFoodnBeverages: 1,
      AvgInflightEntertainment: 1,
      AvgValueForMoney: 1,
      AvgOverallRating: 1}
  },
  {$addFields: {
      sortKey: {
        $switch: {
          branches: [
            { case: { $eq: ["$Class", "First Class"] }, then: 1 },
            { case: { $eq: ["$Class", "Business Class"] }, then: 2 },
            { case: { $eq: ["$Class", "Premium Economy"] }, then: 3 },
            { case: { $eq: ["$Class", "Economy Class"] }, then: 4 }
          ],
          default: 5}
      }
    }
  },
  {$sort: {
      Airline: 1,
      sortKey: 1,
      Seasonality: 1}
  }
]);


//Question 8
db.airlines_reviews.aggregate([
    {$match: {"Recommended": "no"}},
    {
        $group: {
            _id: {"Airline": "$Airline", "TypeofTraveller": "$TypeofTraveller"},
            DelayIssues: {
                $sum: {
                    $cond: [{$regexMatch: {input: "$Reviews", regex: /delay/} }, 1, 0] //need to incl delay, Delay etc
                }
            },
            DiscomfortIssues: {
                $sum: {
                    $cond: [
                        {$or: [
                            {$regexMatch: {input: "$Reviews", regex: /comfort/} },
                        ]}, 1, 0
                    ]
                }
            },
            ServiceIssues: {
                $sum: {
                    $cond: [
                        {$or: [
                            {$regexMatch: {input: "$Reviews", regex: /service/} },
                            {$regexMatch: {input: "$Reviews", regex: /support/} },
                            {$regexMatch: {input: "$Reviews", regex: /baggage/} }
                        ]}, 1, 0
                    ]
                }
            },
            FoodIssues: {
                $sum: {
                    $cond: [
                        {$or: [
                            {$regexMatch: {input: "$Reviews", regex: /food/} },
                            {$regexMatch: {input: "$Reviews", regex: /meal/}}
                        ]}, 1, 0
                    ]
                }
            },
            SeatingIssues: {
                $sum: {
                    $cond: [{$regexMatch: {input: "$Reviews", regex: /seat/} }, 1, 0]
                }
            },
            PreflightIssues: {
                $sum: {
                    $cond: [
                        {$or: [
                            {$regexMatch: {input: "$Reviews", regex: /booking/} },
                            {$regexMatch: {input: "$Reviews", regex: /refund/} },
                            {$regexMatch: {input: "$Reviews", regex: /compensation/} },
                            {$regexMatch: {input: "$Reviews", regex: /check-in/} },
                            {$regexMatch: {input: "$Reviews", regex: /check in/} } //check in without hyphen
                        ]}, 1, 0
                    ]
                }
            }
        }
    },
    {
        $project: {
            DelayIssues: 1,
            DiscomfortIssues: 1,
            ServiceIssues: 1,
            FoodIssues: 1,
            SeatingIssues: 1,
            PreflightIssues: 1
        }
    },
    {
        $sort: {"_id.Airline": 1, "_id.TypeofTraveller": 1}
    }
]);

//Question 9
db.airlines_reviews.aggregate([
    {
        $addFields: {
            Period: {
                $let: {
                    vars: {
                        // Adjust the year by prepending "20" for 21st century dates
                        monthFlownFull: {
                            $concat: [
                                "01-",
                                { $substr: ["$MonthFlown", 0, 3] }, // Extract month (e.g., "Dec")
                                "-20", // Prepend "20" to indicate 21st century
                                { $substr: ["$MonthFlown", 4, 2] }  // Extract the last two digits of year
                            ]
                        }
                    },
                    in: {
                        $cond: [
                            { $lt: [ { $dateFromString: { dateString: "$$monthFlownFull", format: "%d-%b-%Y" } }, ISODate("2020-01-23") ] },
                            "Pre-COVID",
                            {
                                $cond: [
                                    { $gte: [ { $dateFromString: { dateString: "$$monthFlownFull", format: "%d-%b-%Y" } }, ISODate("2021-02-11") ] },
                                    "Post-COVID",
                                    "During-COVID"
                                ]
                            }
                        ]
                    }
                }
            }
        }
    },
    {
        $match: {
            $or: [
                { Period: "Pre-COVID" },
                { Period: "Post-COVID" }
            ]
        }
    },
    {
        $group: {
            _id: "$Period",
            AvgOverallRating: { $avg: "$OverallRating" },
            AvgSeatComfort: { $avg: "$SeatComfort" },
            AvgFoodnBeverages: { $avg: "$FoodnBeverages" },
            AvgInflightEntertainment: { $avg: "$InflightEntertainment" },
            AvgValueForMoney: { $avg: "$ValueForMoney" }
        }
    },
    {
        $project: {
            _id: 0,
            Period: "$_id",
            AvgOverallRating: 1,
            AvgSeatComfort: 1,
            AvgFoodnBeverages: 1,
            AvgInflightEntertainment: 1,
            AvgValueForMoney: 1
        }
    }
])


// Question 10
??