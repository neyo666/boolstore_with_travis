require_relative 'user_command'

class AuthorSearchCommand < UserCommand

	def initialize (data_source)
		super (data_source)
		@author = ''
	end

	def title 
		'Search by author.'
	end

   def input
   	   puts title
	   print "Author mame? "   
	   @author = STDIN.gets.chomp  
   end

    def execute
       result = @data_source.authorSearch(@author)
       result['books'].each {|book| puts "#{book['title']} - #{book['isbn']} " }
       puts "Total value = #{result['value']}"
	end

end