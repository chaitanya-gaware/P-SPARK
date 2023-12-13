# class s3:
#     def __init__(self,key_input,id_input) -> None:
#         self.key = key_input
#         self.id = id_input
        
#     def upload(self):
#         self.name_in_bucket = "bucket_key_name"
#         print(self.key)
#         print(self.id)
#         print(self.name_in_bucket)
        

# du = s3("i am key", "i am key id")

# du.upload()

# class S3:
#     # Class variable (static variable)
#     default_name_in_bucket = "default_bucket_key_name"

#     def __init__(self, key_input, id_input) -> None:
#         # Instance variables
#         self.key = key_input
#         self.id = id_input

#     @classmethod
#     def upload(cls, key, id):
#         # Accessing the class variable
#         name_in_bucket = cls.default_name_in_bucket
#         print(f"Key: {key}, ID: {id}, Name in Bucket: {name_in_bucket}")

# # Object creation
# du = S3("i am key", "i am key id")

# # Calling the class method using the class name
# S3.upload("another key", "another key id")

# # Calling the class method using the object
# du.upload("yet another key", "yet another key id")

#########
class BankAccount:
  """
  Represents a bank account with account number, balance, and owner name.
  """

  # Static variable to store the next available account number
  next_account_number = 1000

  def __init__(self, owner_name, initial_balance=0):
    """
    Initializes a new bank account with owner name and initial balance.
    """
    self.account_number = BankAccount.next_account_number
    BankAccount.next_account_number += 1
    self.owner_name = owner_name
    self.balance = initial_balance

  # Instance method
  def deposit(self, amount):
    """
    Deposits a specified amount into the account.
    """
    self.balance += amount
    print(f"Deposited ${amount}. New balance: ${self.balance}.")

  # Instance method
  def withdraw(self, amount):
    """
    Withdraws a specified amount from the account if sufficient funds exist.
    """
    if self.balance < amount:
      print("Insufficient funds.")
    else:
      self.balance -= amount
      print(f"Withdrawn ${amount}. New balance: ${self.balance}.")

  # Static method
  @staticmethod
  def get_interest_rate():
    """
    Returns the current interest rate for bank accounts.
    """
    return 0.05

  # Class method
  @classmethod
  def create_account_with_balance(cls, owner_name, balance):
    """
    Creates a new bank account with a specified owner name and balance.
    """
    new_account = cls(owner_name)
    new_account.deposit(balance)
    return new_account

# Create a new bank account
account1 = BankAccount("John Doe")

# Print the account number
print(f"Account number: {account1.account_number}")

# Print the owner name
print(f"Owner name: {account1.owner_name}")

# Print the initial balance
print(f"Initial balance: ${account1.balance}")

# Deposit some money
account1.deposit(100)

# Print the updated balance
print(f"Updated balance: ${account1.balance}")

# Get the current interest rate
interest_rate = BankAccount.get_interest_rate()
print(f"Current interest rate: {interest_rate:.2%}")

# Create a new account with initial balance using class method
account2 = BankAccount.create_account_with_balance("Jane Doe", 500)

# Print the account details
print(f"\nAccount 2:")
print(f"Account number: {account2.account_number}")
print(f"Owner name: {account2.owner_name}")
print(f"Balance: ${account2.balance}")

