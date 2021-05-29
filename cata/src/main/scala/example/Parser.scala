import cats._

object ParseMe {
    type ParserState = (String, Int)

    sealed trait Parser[T] {
        def consume(state: ParserState): (ParserState, ParseResult[T])
    }

    sealed trait ParseResult[T]
    case class ParseError[T]() extends ParseResult[T]
    case class ParseSuccess[T](t: T) extends ParseResult[T]

    def charParser(a: Char) = new Parser[Char] {
        def consume(parserState: ParserState): (ParserState, ParseResult[Char]) = {
            val (source, pos) = parserState
            val currentChar = source(pos)

            if (currentChar == a) {
                ((source, pos + 1), ParseSuccess(a))
            } else {
                (parserState, ParseError())
            }

        }
    }

}