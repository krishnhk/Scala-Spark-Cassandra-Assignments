import scala.annotation.tailrec

object FractalTree extends App {

 def constructTree(itr: Int): Unit = {
   def iteratingTree(tree: Array[Array[Char]],rec:Int, base: Int=62, height: Int=16, rootLoc: Int=49): Array[Array[Char]] = {

     if (rec > 0 && height>0) {
       @tailrec
       def constructRoot(tree: Array[Array[Char]], stem: Int, stemLen: Int, loc: Int): Unit = {
         tree(stem - stemLen + 1)(loc) = '*'
         if (stemLen > 1) constructRoot(tree, stem, stemLen - 1, loc)
       }

       @tailrec
       def constructBranch(leftBranch: Int, rightBranch: Int, branchHeight: Int, branchBase: Int): Unit = {
         tree(branchBase)(rightBranch) = '*'
         tree(branchBase)(leftBranch) = '*'
         if (branchHeight > 1) constructBranch(leftBranch - 1, rightBranch + 1, branchHeight - 1, branchBase - 1)
       }
       constructBranch(rootLoc - 1, rootLoc + 1, height, base - height)
       constructRoot(tree, base, height, rootLoc)
       iteratingTree(tree,rec - 1, base - (height * 2), height / 2, rootLoc + height )
       iteratingTree(tree,rec - 1, base - (height * 2), height / 2, rootLoc - height)
     }
     tree
   }
   print(iteratingTree(Array.fill(63, 100)('.'),itr).map(_.mkString).mkString("\n"))
 }
 constructTree(scala.io.StdIn.readInt())
}
