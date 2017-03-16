class LocalCache

	def initialize()
		@storage = {}
	end

	def set key, value
		@storage[key] = value
	end

	def get key 
	    @storage[key]
	end

end
