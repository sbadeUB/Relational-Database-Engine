package edu.buffalo.cse562;


public class TreeNode {

	String data;
	TreeNode leftChild;
	TreeNode rightChild;
	TreeNode parent;
	static int ParenthCount=0;
	public TreeNode(String data) {
		this.data=data;
		this.parent=null;
		this.leftChild=null;
		this.rightChild=null;
	}
	public boolean AddChilds(String leftchildData,String rightchildData)
	{
		TreeNode leftchild=new TreeNode(leftchildData);
		TreeNode rightchild=null;
		if(rightchildData!=null || rightchildData!="")
		rightchild=new TreeNode(rightchildData);
		this.leftChild=leftchild;
		this.rightChild=rightchild;
		leftchild.parent=this;
		rightchild.parent=this;
		return true;		
	}
	public boolean AddChilds(TreeNode leftchild,String rightchildData)
	{
		
		TreeNode rightchild=null;
		if(rightchildData!=null || rightchildData!="")
		rightchild=new TreeNode(rightchildData);
		this.leftChild=leftchild;
		this.rightChild=rightchild;
		leftchild.parent=this;
		rightchild.parent=this;
		return true;		
	}
	public boolean AddRightChild(TreeNode rightchild)
	{		
		this.rightChild=rightchild;
		rightchild.parent=this;
		return true;		
	}
	/* Reference:http://rosettacode.org/wiki/Tree_traversal */
	public static void preorder(TreeNode n)
	 {
	  if (n != null)
	  {
		  if(n.data!=null)
	   System.out.println(n.data + " ");
	   preorder(n.leftChild);
	   preorder(n.rightChild);
	  }
	 }
	 public TreeNode getRight() {
		 
		return this.rightChild;
	}
	public TreeNode getLeft() {
		ParenthCount++;
		return this.leftChild;
	}
	

}
