require_relative 'user_command'

class UpdateBookCommand < UserCommand

	def initialize (data_source)
		super (data_source)
		@isbn = ''
	end

	def title 
		'Update Book.'
	end

  def input
   	 puts title
	   print "ISBN? "   
	   @isbn = STDIN.gets.chomp  
  end

  def execute
     book = @data_source.isbnSearch @isbn
     if book
        puts '[Hit <return> key to skip to next field]'
        print "Author (#{book.author}) ?"
        response = STDIN.gets.chomp 
        book.author = response if response.length > 0 
        print "Title (#{book.title}) ?"
        response = STDIN.gets.chomp 
        book.title = response if response.length > 0 
        print "Price (#{book.price}) ?"
        response = STDIN.gets.chomp 
        book.price = response.to_f if response.length > 0 
        @data_source.updateBook book   # NEW
      else
        puts 'Invalid ISBN'
      end
	end 

end
