require_relative 'user_command'

class ISBNSearchCommand < UserCommand

	def initialize (data_source)
		super (data_source)
		@isbn = ''
	end

	def title 
		'ISBN Search'
	end

   def input
   	    puts title
		print "Enter an ISBN? "   
		@isbn = STDIN.gets.chomp  
   end

    def execute
    	result = @data_source.isbnSearch(@isbn)
    	if result
    	    puts result
    	else
    		puts "Invalid ISBN"
    	end
	end

end