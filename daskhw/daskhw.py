from dask import delayed, compute
import time


# This function calculates the number of times a squared sequence value starts with a specific base for a given range.
def sequence_serial(num, base):
    count = 0
    for n in range(1, num + 1):  # Iterating through the sequence.
        seq_value = n ** 2 * (n + 1) ** 2  # Calculating the sequence value.
        if str(seq_value).startswith(str(base)):  # Checking if the sequence value starts with the desired base.
            count += 1  # Incrementing the count if condition is met.
    return count


# Parallelized version of the sequence calculation using Dask.
def sequence_parallel(num, base):
    @delayed
    def seq_value(n):
        return n ** 2 * (n + 1) ** 2  # Computing the sequence value.

    values = [seq_value(n) for n in range(1, num + 1)]

    computed_values = compute(*values)

    count = sum(1 for val in computed_values if str(val).startswith(str(base)))
    return count


# This function tests both the serial and parallel versions to ensure they produce the same result.
def test_functions():
    assert sequence_serial(7, 4) == 2  # Testing the serial version.
    assert sequence_parallel(7, 4) == 2  # Testing the parallel version.
    print("All tests passed!")  # If no assertions fail, this line will execute.


# This function checks the performance difference between the serial and parallel implementations.
def performance_analysis():
    start = time.time()  # Starting the timer.
    sequence_serial(10000, 4)  # Running the serial version.
    end = time.time()  # Ending the timer.
    print(f"Time taken by sequence_serial: {end - start:.2f} seconds")  # Printing the time taken.

    start = time.time()  # Starting the timer.
    sequence_parallel(10000, 4)  # Running the parallel version.
    end = time.time()  # Ending the timer.
    print(f"Time taken by sequence_parallel: {end - start:.2f} seconds")  # Printing the time taken.


# The main entry point of the script.
if __name__ == '__main__':
    test_functions()  # Testing the functions.
    performance_analysis()  # Checking the performance difference.
