class ConnectionDataBase:
    """A base class to be used by specialized ConnectionData implementations."""

    def fill_properties(self, values_dict: dict, property_names) -> None:
        """
        Fills the properties that match the given property_names with the
        corresponding values in the dictionary.

        """

        if values_dict is not None:
            for property_name in property_names:
                value = values_dict.get(property_name, None)
                if value is not None:
                    setattr(self, property_name, value)
