/*
This package is required to provide functionality to process a csv file and return a sorted (by email domain) data
structure of your choice containing the email domains along with the number of customers for each domain. The customer_data.csv
file provides an example csv file to work with. Any errors should be logged (or handled) or returned to the consumer of
this package. Performance matters, the sample file may only contain 1K lines but the package may be expected to be used on
files with 10 million lines or run on a small machine.

Write this package as you normally would for any production grade code that would be deployed to a live system.

Please stick to using the standard library.
 */

package emaildomainstats
