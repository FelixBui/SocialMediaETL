    def execute(self):
        transform_data = self.transform
        self.load(transform_data)
        return super().execute()