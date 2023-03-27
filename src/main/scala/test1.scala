import com.ilotterytech.ocean.dp.D2D3.{Game, GameFactory}

object test1 {

  def main(args: Array[String]): Unit = {

    val game: Game = GameFactory.getInstance().getGame("10005")
    println(game.getGameId)
    println(game.getGameType)
  }

}
