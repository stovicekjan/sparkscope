class MetricDetailsList:
    def __init__(self, ascending):
        """
        Create MetricDetailsList object
        :param ascending: False if "the larger, the more important" applies to the sorting attribute. Otherwise True.
        """
        self._details_list = []
        self.ascending = ascending

    def add(self, details):
        """
        Add new MetricDetails into the list.
        :param details: MetricDetails
        """
        if not isinstance(details, MetricDetails):
            raise TypeError("Cannot add the object to the MetricDetailsList - must be a MetricDetails object.")
        self._details_list.append(details)

    def length(self):
        """
        Get a number of elements in the list
        :return: a number of elements in the list
        """
        return len(self._details_list)

    def get_short_list(self, length=-1):
        """
        Get a specified number of most important MetricDetails, sorted by severity
        :param length: Number of items that should be retrieved, -1 for all of them
        :return: list of most important MetricDetails or None
        """
        def get_sort_attr(elem):
            return elem.sort_attr
        sorted_list = sorted(self._details_list, key=get_sort_attr, reverse=self.ascending)
        if length == -1 or length >= len(sorted_list):
            return sorted_list
        elif length < len(sorted_list):
            return sorted_list[0:length]
        else:
            ValueError(f"Invalid MetricDetailsList length: {length}.")


class MetricDetails:
    def __init__(self, entity_id: object, detail_string: object, sort_attr: object = 0, subdetails: object = []) -> object:
        """
        Class holding details of a metric
        :param entity_id: id of an entity described by the metric (e.g. Stage or Executor)
        :param detail_string: String containing details about the metric (a one liner)
        :param sort_attr: Attribute determination what contributes the most to the severity
        :param subdetails: List of strings with additional details (may occupy multiple lines)
        """
        self.id = entity_id
        self.detail_string = detail_string
        self.sort_attr = sort_attr
        self.subdetails = subdetails
