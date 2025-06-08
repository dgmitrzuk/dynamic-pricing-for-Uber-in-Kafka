import pandas as pd

def calculate_price(x):
    # if you passed a 1×N DataFrame, turn it into a Series
    if isinstance(x, pd.DataFrame):
        x = x.squeeze()
    
    base_price = 15.0
    duration_charge = x['Expected_Ride_Duration'] * 0.7

    # time multiplier
    if x['Time_of_Booking'] == "Night":
        tm = 1.7
    elif x['Time_of_Booking'] == "Evening":
        tm = 1.3
    else:
        tm = 1.1
    time_charge = base_price * tm

    # vehicle multiplier
    vm = {
        "Luxury": 2.0,
        "Premium": 1.5,
        "SUV": 1.2
    }.get(x['Vehicle_Type'], 1.05)
    vehicle_charge = base_price * vm

    return base_price + duration_charge + time_charge + vehicle_charge
