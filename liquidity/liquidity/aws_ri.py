import boto3
import json

class AwsRIData:
    def __init__(self):
        # TODO: support different regions
        self.ec2 = boto3.client("ec2")
        self.database_file = "database.json"
        self.load_database()

        # TODO retrieve configuration using describe_instance_types
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2/client/describe_instance_types.html
        self.instance_config = {
            "m5": {
                "sizes": ["large", "xlarge", "2xlarge", "4xlarge", "8xlarge", "12xlarge", "16xlarge", "24xlarge", "metal"],
            },
            "c5": {
                "sizes": ["large", "xlarge", "2xlarge", "4xlarge", "9xlarge", "12xlarge", "18xlarge", "24xlarge", "metal"],
            },
        }

    def load_database(self):
        try:
            self.database = json.load(open(self.database_file))
        except FileNotFoundError:
            self.database = {}

    def commit_database(self):
        json.dump(self.database, open(self.database_file, "w"))

    """
    Return the maximum number of available marketplace instances for some instance type, of all the instance types available in the database
    TODO Aggregate data like this will be pre-computed and stored in the database
    For now - compute on the fly
    """
    def get_max_instance_type_availability(self):
        instance_availabilities = [self.get_instance_type_available_marketplace_instances(type) for type in self.instance_config.keys()]
        return max(instance_availabilities)

    """
    TODO extract to separate class or package
    Get marketplace offerings for a given instance type and size from AWS
    Will be executed periodically in
    """
    def get_marketplace_offerings_from_aws(self, instance_type_size: str = "m5.large"):
        paginator = self.ec2.get_paginator('describe_reserved_instances_offerings')
        page_iterator = paginator.paginate(
            InstanceType=instance_type_size,
            IncludeMarketplace=True,
            Filters=[
                {
                    "Name": "marketplace",
                    "Values": ["true"],
                },
            ],
        )
        offerings = []
        for page in page_iterator:
            offerings += page["ReservedInstancesOfferings"]
        return offerings

    """
    Return number of available marketplace instances for a given instance type, across sizes
    """
    def get_instance_type_available_marketplace_instances(self, instance_type: str = "m5"):
        instances_available = 0
        for offering_instance_type, offerings in self.database.items():
            if not offering_instance_type.startswith(f"{instance_type}."):
                continue
            for o in offerings:
                for p in o["PricingDetails"]:
                    instances_available += p["Count"]
        return instances_available

    """
    This will power the API method returning the relative liquidity
    Value [0,1], 1 = maximum liquidity
    """
    def get_instance_type_liquidity(self, instance_type: str = "m5"):
        return self.get_instance_type_available_marketplace_instances(instance_type) / self.get_max_instance_type_availability()
    
    """
    Will be driven by batch job to refresh the database with current state of marketplace
    """
    def get_offerings_for_instance_type(self, type: str = "m5"):
        sizes = self.instance_config[type]["sizes"]
        for size in sizes:
            print(f"Getting offerings for {type}.{size}")
            instance_type_size = f"{type}.{size}"
            self.database[instance_type_size] = self.get_marketplace_offerings_from_aws(instance_type_size)
        self.commit_database()


if __name__ == "__main__":
    ri_data = AwsRIData()

    # Get data from AWS - uncomment to refresh data
    # ri_data.get_offerings_for_instance_type("c5")
    # ri_data.get_offerings_for_instance_type("m5")

    # Get liquidity based on data in the database
    print(ri_data.get_instance_type_liquidity("m5"))