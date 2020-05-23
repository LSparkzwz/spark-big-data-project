object App{
  def main(args: Array[String]) {
    if(args(0) == "1"){
      println("Running job 1")
      JobOne.run()
    }else{
      println("Running job 3")
      JobThree.run()
    }
  }
}



